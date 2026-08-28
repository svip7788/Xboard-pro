<?php

namespace Plugin\Epay;

use App\Services\Plugin\AbstractPlugin;
use App\Contracts\PaymentInterface;
use App\Models\Order;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

class Plugin extends AbstractPlugin implements PaymentInterface
{
    public function boot(): void
    {
        $this->filter('available_payment_methods', function ($methods) {
            if ($this->getConfig('enabled', true)) {
                $methods['EPay'] = [
                    'name' => $this->getConfig('display_name', '易支付'),
                    'icon' => $this->getConfig('icon', '💳'),
                    'plugin_code' => $this->getPluginCode(),
                    'type' => 'plugin'
                ];
            }
            return $methods;
        });
    }

    public function form(): array
    {
        return [
            'url' => [
                'label' => '支付网关地址',
                'type' => 'string',
                'required' => true,
                'description' => '请填写完整的支付网关地址，包括协议（http或https）'
            ],
            'pid' => [
                'label' => '商户ID',
                'type' => 'string',
                'description' => '请填写商户ID',
                'required' => true
            ],
            'key' => [
                'label' => '通信密钥',
                'type' => 'string',
                'required' => true,
                'description' => '请填写通信密钥'
            ],
            'type' => [
                'label' => '支付类型',
                'type' => 'string',
                'description' => '支付类型，如: alipay, wxpay, qqpay 等，可自定义'
            ],
            'query_api' => [
                'label' => '查单接口地址',
                'type' => 'string',
                'description' => '留空则用「支付网关地址 + /api/EasyPay/queryOrder」。入账前会调它复核订单是否真的支付成功；填 off 可关闭复核'
            ],
        ];
    }

    public function pay($order): array
    {
        $params = [
            'money' => $order['total_amount'] / 100,
            'name' => $order['trade_no'],
            'notify_url' => $order['notify_url'],
            'return_url' => $order['return_url'],
            'out_trade_no' => $order['trade_no'],
            'pid' => $this->getConfig('pid')
        ];

        if ($paymentType = $this->getConfig('type')) {
            $params['type'] = $paymentType;
        }

        ksort($params);
        $str = stripslashes(urldecode(http_build_query($params))) . $this->getConfig('key');
        $params['sign'] = md5($str);
        $params['sign_type'] = 'MD5';

        return [
            'type' => 1,
            'data' => $this->getConfig('url') . '/submit.php?' . http_build_query($params)
        ];
    }

    public function notify($params): array|bool
    {
        $sign = (string) ($params['sign'] ?? '');
        unset($params['sign'], $params['sign_type']);
        ksort($params);
        $str = stripslashes(urldecode(http_build_query($params))) . $this->getConfig('key');

        if (!hash_equals(md5($str), $sign)) {
            return false;
        }

        $tradeNo = (string) ($params['out_trade_no'] ?? '');

        // 网关用同一套参数和签名回跳用户浏览器，签名有效不等于付款成功，
        // 否则用户可以把失败的回跳原样重放到这里换取开通。
        if (($params['trade_status'] ?? '') !== 'TRADE_SUCCESS') {
            return $this->rejectNotify($tradeNo, 'trade_status', $params['trade_status'] ?? null);
        }

        if (isset($params['pid']) && (string) $params['pid'] !== (string) $this->getConfig('pid')) {
            return $this->rejectNotify($tradeNo, 'pid', $params['pid']);
        }

        $order = Order::where('trade_no', $tradeNo)->first();
        if (!$order) {
            return $this->rejectNotify($tradeNo, 'order_not_found', null);
        }

        // 只拦少付，多付照常开通，避免网关金额格式差异误伤真实回调。
        if (isset($params['money'])) {
            $payable = (int) $order->total_amount + (int) $order->handling_amount;
            $paid = (int) round(((float) $params['money']) * 100);
            if ($paid < $payable - 1) {
                return $this->rejectNotify($tradeNo, 'money', $params['money']);
            }
        }

        // 上面几层都建立在「签名可信」上，密钥一旦泄露就全部失效，
        // 所以入账前再回问一次网关这单到底收没收到钱。
        if ($this->queryGatewayStatus($tradeNo) === false) {
            return $this->rejectNotify($tradeNo, 'gateway_query', 'not success');
        }

        return [
            'trade_no' => $tradeNo,
            'callback_no' => $params['trade_no']
        ];
    }

    /**
     * true = 网关确认已支付，false = 网关确认未支付，null = 问不到（放行并记日志）
     */
    private function queryGatewayStatus(string $tradeNo): ?bool
    {
        $api = trim((string) $this->getConfig('query_api', ''));
        if (strcasecmp($api, 'off') === 0) {
            return null;
        }
        if ($api === '') {
            $api = rtrim((string) $this->getConfig('url'), '/') . '/api/EasyPay/queryOrder';
        }

        $last = null;
        // 刚付完可能还没落库，隔一秒再问，别把真实付款拦掉。
        for ($i = 0; $i < 3; $i++) {
            if ($i > 0) {
                sleep(1);
            }

            try {
                $body = Http::timeout(6)->asForm()->post($api, ['orderNo' => $tradeNo])->json();
            } catch (\Throwable $e) {
                Log::warning('EPay: 查单接口请求失败', ['trade_no' => $tradeNo, 'error' => $e->getMessage()]);
                return null;
            }

            if (!is_array($body) || (int) ($body['code'] ?? 0) !== 1) {
                Log::warning('EPay: 查单接口返回异常', ['trade_no' => $tradeNo, 'body' => $body]);
                return null;
            }

            $status = $body['data']['status'] ?? null;
            if (!is_string($status) || $status === '') {
                Log::warning('EPay: 查单接口无状态字段', ['trade_no' => $tradeNo, 'body' => $body]);
                return null;
            }

            if (strcasecmp($status, 'success') === 0) {
                return true;
            }
            $last = $status;
        }

        Log::warning('EPay: 网关查单未支付', ['trade_no' => $tradeNo, 'status' => $last]);
        return false;
    }

    private function rejectNotify(string $tradeNo, string $reason, $value): bool
    {
        Log::warning('EPay: 签名有效但回调内容不可信', [
            'trade_no' => $tradeNo,
            'reason' => $reason,
            'value' => $value
        ]);

        return false;
    }
}