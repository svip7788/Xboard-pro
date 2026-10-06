<?php

namespace App\Services;

use App\Exceptions\ApiException;
use App\Models\Plan;
use App\Models\TrafficResetLog;
use App\Models\User;
use Carbon\Carbon;
use Illuminate\Support\Facades\DB;

/**
 * 用剩余订阅时长兑换流量重置
 */
class TrafficExchangeService
{
    // 扣除本周期剩余时间，新周期从现在开始
    public const MODE_REMAIN = 'remain';
    // 扣除一个月到期时间，重置日不变
    public const MODE_MONTH = 'month';

    public const MODES = [self::MODE_REMAIN, self::MODE_MONTH];

    public function __construct(
        private readonly TrafficResetService $trafficResetService
    ) {
    }

    public function isEnabled(): bool
    {
        return (bool) admin_setting('traffic_exchange_enable', 0);
    }

    public function getThreshold(): int
    {
        return max(1, min(100, (int) admin_setting('traffic_exchange_threshold', 90)));
    }

    public function getOptions(User $user): array
    {
        $enabled = $this->isEnabled();
        $plan = $user->plan;
        $active = $plan && app(UserService::class)->isAvailable($user);
        $reached = $this->reachedThreshold($user);
        $now = Carbon::now(config('app.timezone'));

        $modes = [];
        foreach (self::MODES as $mode) {
            $modes[$mode] = $enabled && $active && $reached ? $this->calculate($user, $mode, $now) : null;
        }
        $resetPrice = $active ? ($plan->prices[Plan::PERIOD_RESET_TRAFFIC] ?? null) : null;

        return [
            'enable' => $enabled,
            'plan_id' => $user->plan_id,
            'threshold' => $this->getThreshold(),
            'used' => $user->getTotalUsedTraffic(),
            'total' => (int) $user->transfer_enable,
            'reached' => $reached,
            'expired_at' => $user->expired_at,
            'next_reset_at' => $user->next_reset_at,
            // 单位：分，与套餐接口的旧版价格字段一致
            'reset_price' => $resetPrice !== null ? (int) round((float) $resetPrice * 100) : null,
            'restart_available' => $enabled && $active && (bool) $plan->renew,
            'modes' => $modes,
        ];
    }

    /**
     * @throws ApiException
     */
    public function exchange(User $user, string $mode): array
    {
        if (!$this->isEnabled()) {
            throw new ApiException(__('Traffic exchange is not enabled'));
        }

        return DB::transaction(function () use ($user, $mode) {
            $fresh = User::lockForUpdate()->find($user->id);
            if (!$fresh || !$fresh->plan || !app(UserService::class)->isAvailable($fresh)) {
                throw new ApiException(__('Subscription has expired or no active subscription'));
            }
            if (!$this->reachedThreshold($fresh)) {
                throw new ApiException(__('Traffic usage has not reached the exchange threshold'));
            }

            $result = $this->calculate($fresh, $mode, Carbon::now(config('app.timezone')));
            if (!$result) {
                throw new ApiException(__('Not enough remaining subscription time to exchange'));
            }

            $reset = $this->trafficResetService->performReset($fresh, TrafficResetLog::SOURCE_TIME_EXCHANGE, [
                'mode' => $mode,
                'old_expired_at' => (int) $fresh->expired_at,
                'new_expired_at' => $result['new_expired_at'],
                'deduct_seconds' => $result['deduct_seconds'],
            ], $result['new_expired_at']);

            if (!$reset) {
                throw new ApiException(__('Exchange failed, please try again later'));
            }

            return $result;
        });
    }

    private function reachedThreshold(User $user): bool
    {
        $total = (int) $user->transfer_enable;
        return $total > 0 && $user->getTotalUsedTraffic() * 100 >= $total * $this->getThreshold();
    }

    /**
     * 计算兑换后的到期时间，不满足条件返回 null
     */
    private function calculate(User $user, string $mode, Carbon $now): ?array
    {
        if (!$user->plan || $user->expired_at === null || $user->next_reset_at === null) {
            return null;
        }

        $tz = config('app.timezone');
        $expiredAt = Carbon::createFromTimestamp((int) $user->expired_at, $tz);
        $nextResetAt = Carbon::createFromTimestamp((int) $user->next_reset_at, $tz);
        if ($expiredAt->lte($now) || $nextResetAt->lte($now)) {
            return null;
        }

        $method = $this->trafficResetService->getEffectiveResetMethod($user->plan);

        if ($mode === self::MODE_REMAIN) {
            // 按到期日锚定重置日的套餐，到期日与下次重置日之间正好是整数个周期
            if ($method !== Plan::RESET_TRAFFIC_MONTHLY) {
                return null;
            }
            $cycles = ($expiredAt->year - $nextResetAt->year) * 12 + $expiredAt->month - $nextResetAt->month;
            if ($cycles < 1) {
                return null;
            }
            $newExpiredAt = $now->copy()->addMonthsNoOverflow($cycles);
        } elseif ($mode === self::MODE_MONTH) {
            if (!in_array($method, [Plan::RESET_TRAFFIC_MONTHLY, Plan::RESET_TRAFFIC_FIRST_DAY_MONTH], true)) {
                return null;
            }
            $newExpiredAt = $expiredAt->copy()->subMonthNoOverflow();
            if ($newExpiredAt->lt($nextResetAt)) {
                return null;
            }
        } else {
            return null;
        }

        if ($newExpiredAt->gte($expiredAt)) {
            return null;
        }

        $preview = clone $user;
        $preview->expired_at = $newExpiredAt->timestamp;
        $newNextResetAt = $this->trafficResetService->calculateNextResetTime($preview);

        return [
            'mode' => $mode,
            'new_expired_at' => $newExpiredAt->timestamp,
            'new_next_reset_at' => $newNextResetAt?->timestamp,
            'deduct_seconds' => $expiredAt->timestamp - $newExpiredAt->timestamp,
        ];
    }
}
