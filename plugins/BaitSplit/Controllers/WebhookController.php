<?php

namespace Plugin\BaitSplit\Controllers;

use Illuminate\Contracts\Cache\LockTimeoutException;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Routing\Controller;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;
use InvalidArgumentException;
use Plugin\BaitSplit\Services\BaitSplitService;
use RuntimeException;
use Throwable;

class WebhookController extends Controller
{
    public function rotateIp(Request $request): JsonResponse
    {
        $service = BaitSplitService::fromDatabase();
        try {
            $service->assertIpWebhookSignature(
                (string) $request->header('X-Bait-Timestamp', ''),
                $request->getContent(),
                (string) $request->header('X-Bait-Signature', '')
            );
        } catch (RuntimeException $exception) {
            return $this->error($exception->getMessage(), 503);
        } catch (InvalidArgumentException $exception) {
            return $this->error($exception->getMessage(), 401);
        }

        $data = $request->validate([
            'event_id' => [
                'required',
                'string',
                'max:100',
                'regex:/^[A-Za-z0-9._:-]+$/',
            ],
            'campaign_id' => ['required', 'string', 'max:64'],
            'instance_id' => ['nullable', 'string', 'max:100'],
            'target_id' => [
                'required',
                'string',
                'max:100',
                'regex:/^[A-Za-z0-9._:-]+$/',
            ],
            'old_ip' => ['nullable', 'ipv4'],
            'new_ip' => ['required', 'ipv4'],
            'reason' => ['nullable', 'string', 'max:20'],
        ]);
        $eventKey = 'bait_split:ip_rotate_event:'
            . hash('sha256', $data['campaign_id'] . ':' . $data['event_id']);

        try {
            // 换 IP 最高优先级：多等一会儿；仍抢不到则入队，绝不丢事件
            $result = Cache::lock('bait_split:admin_state', 45)->block(
                25,
                function () use ($service, $data, $eventKey): array {
                    $cached = Cache::get($eventKey);
                    if (is_array($cached)) {
                        $cached['duplicate'] = true;
                        return $cached;
                    }
                    $result = $service->rotateCampaignIp(
                        $data['campaign_id'],
                        $data['old_ip'] ?? '',
                        $data['new_ip'],
                        $data['target_id'] ?? '',
                        $data['reason'] ?? 'blocked'
                    );
                    $result['event_id'] = $data['event_id'];
                    $result['instance_id'] = $data['instance_id'] ?? '';
                    $result['duplicate'] = false;
                    Cache::put($eventKey, $result, now()->addDay());
                    Log::notice('BaitSplit AWS 公网 IP 已自动轮换', $result);
                    return $result;
                }
            );
            return response()->json(['ok' => true, 'data' => $result]);
        } catch (InvalidArgumentException $exception) {
            return $this->error($exception->getMessage(), 422);
        } catch (LockTimeoutException) {
            try {
                $pending = $service->enqueueIpRotate($data);
                Log::warning('BaitSplit 换 IP 锁忙，已入待处理队列', [
                    'event_id' => $data['event_id'],
                    'target_id' => $data['target_id'],
                    'new_ip' => $data['new_ip'],
                    'pending' => $pending,
                ]);
                return response()->json([
                    'ok' => true,
                    'queued' => true,
                    'data' => [
                        'event_id' => $data['event_id'],
                        'pending' => $pending,
                    ],
                ], 202);
            } catch (Throwable $exception) {
                Log::error('BaitSplit 换 IP 入队失败', [
                    'event_id' => $data['event_id'],
                    'error' => $exception->getMessage(),
                ]);
                return $this->error('IP 轮换繁忙且入队失败，请重试', 503);
            }
        } catch (Throwable $exception) {
            Log::error('BaitSplit AWS IP 轮换失败', [
                'event_id' => $data['event_id'],
                'error' => $exception->getMessage(),
            ]);
            return $this->error('IP 轮换失败，请查看服务端日志', 500);
        }
    }

    private function error(string $message, int $status): JsonResponse
    {
        return response()->json([
            'ok' => false,
            'error' => $message,
        ], $status);
    }
}
