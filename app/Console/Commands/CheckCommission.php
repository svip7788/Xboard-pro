<?php

namespace App\Console\Commands;

use App\Models\CommissionLog;
use Illuminate\Console\Command;
use App\Models\Order;
use App\Models\User;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class CheckCommission extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'check:commission';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = '返佣服务';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        $this->autoCheck();
        $this->autoPayCommission();
    }

    public function autoCheck()
    {
        if ((int)admin_setting('commission_auto_check_enable', 1)) {
            Order::where('commission_status', 0)
                ->where('invite_user_id', '!=', NULL)
                ->where('status', 3)
                ->where('updated_at', '<=', strtotime('-3 day', time()))
                ->update([
                    'commission_status' => 1
                ]);
        }
    }

    public function autoPayCommission()
    {
        $orders = Order::where('commission_status', 1)
            ->where('invite_user_id', '!=', NULL)
            ->get();

        foreach ($orders as $order) {
            // 抢占式幂等：只有一次 UPDATE 能把 1 改成 2，多个 cron 并发也只有一个会进入 payHandle。
            $claimed = Order::where('id', $order->id)
                ->where('commission_status', 1)
                ->update(['commission_status' => 2]);

            if ($claimed === 0) {
                continue;
            }

            try {
                DB::transaction(function () use ($order) {
                    if (!$this->payHandle($order->invite_user_id, $order)) {
                        throw new \RuntimeException('payHandle returned false');
                    }
                    // commission_status 已在抢占阶段落库;这里只是刷 actual_commission_balance 等
                    $order->commission_status = 2;
                    if (!$order->save()) {
                        throw new \RuntimeException('order save failed');
                    }
                });
            } catch (\Throwable $e) {
                // 打款失败 → 回滚 commission_status 等下一轮再尝试
                Order::where('id', $order->id)
                    ->where('commission_status', 2)
                    ->update(['commission_status' => 1]);
                Log::error('commission pay failed', [
                    'order_id' => $order->id,
                    'trade_no' => $order->trade_no,
                    'error' => $e->getMessage(),
                ]);
            }
        }
    }

    public function payHandle($inviteUserId, Order $order)
    {
        $level = 3;
        if ((int)admin_setting('commission_distribution_enable', 0)) {
            $commissionShareLevels = [
                0 => (int)admin_setting('commission_distribution_l1'),
                1 => (int)admin_setting('commission_distribution_l2'),
                2 => (int)admin_setting('commission_distribution_l3')
            ];
        } else {
            $commissionShareLevels = [
                0 => 100
            ];
        }
        for ($l = 0; $l < $level; $l++) {
            $inviter = User::find($inviteUserId);
            if (!$inviter) continue;
            if (!isset($commissionShareLevels[$l])) continue;
            $commissionBalance = $order->commission_balance * ($commissionShareLevels[$l] / 100);
            if (!$commissionBalance) continue;
            if ((int)admin_setting('withdraw_close_enable', 0)) {
                $inviter->increment('balance', $commissionBalance);
            } else {
                $inviter->increment('commission_balance', $commissionBalance);
            }
            if (!$inviter->save()) {
                // 外层 DB::transaction 会在我们返回 false 后触发异常并整体回滚
                return false;
            }
            CommissionLog::create([
                'invite_user_id' => $inviteUserId,
                'user_id' => $order->user_id,
                'trade_no' => $order->trade_no,
                'order_amount' => $order->total_amount,
                'get_amount' => $commissionBalance
            ]);
            $inviteUserId = $inviter->invite_user_id;
            // update order actual commission balance
            $order->actual_commission_balance = $order->actual_commission_balance + $commissionBalance;
        }
        return true;
    }

}
