<?php

namespace App\Services;

use App\Exceptions\ApiException;
use App\Models\Coupon;
use App\Models\Order;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;

class CouponService
{
    public $coupon;
    public $planId;
    public $userId;
    public $period;

    public function __construct($code)
    {
        // 原代码在构造函数里 lockForUpdate，但若调用方不在事务中，
        // 这个锁立刻被释放，并无实际并发保护作用。
        // 改为普通查询，实际并发保护移到 limit_use 递减那一步用原子 UPDATE 完成。
        $this->coupon = Coupon::where('code', $code)->first();
    }

    public function use(Order $order): bool
    {
        $this->setPlanId($order->plan_id);
        $this->setUserId($order->user_id);
        $this->setPeriod($order->period);
        // 下单时才占用单用户配额（check() 只做静态校验，不副作用，避免「验证后无法付款」）
        $this->check();
        if (!$this->acquireUserQuota()) {
            throw new ApiException(__('The coupon can only be used :limit_use_with_user per person', [
                'limit_use_with_user' => $this->coupon->limit_use_with_user
            ]));
        }
        switch ($this->coupon->type) {
            case 1:
                $order->discount_amount = $this->coupon->value;
                break;
            case 2:
                $order->discount_amount = $order->total_amount * ($this->coupon->value / 100);
                break;
        }
        if ($order->discount_amount > $order->total_amount) {
            $order->discount_amount = $order->total_amount;
        }
        if ($this->coupon->limit_use !== NULL) {
            // 原子递减：只有 limit_use > 0 时才能扣减，影响行数==0 说明已被别的请求领完。
            // 这样即使无事务上下文，两个并发请求也只有一个能领到最后一张券。
            $affected = Coupon::where('id', $this->coupon->id)
                ->where('limit_use', '>', 0)
                ->decrement('limit_use');
            if ($affected === 0) {
                // 全局额度已空，归还之前占位的单用户配额
                $this->releaseUserQuota();
                return false;
            }
            $this->coupon->limit_use = max(0, $this->coupon->limit_use - 1);
        }
        return true;
    }

    public function getId()
    {
        return $this->coupon->id;
    }

    public function getCoupon()
    {
        return $this->coupon;
    }

    public function setPlanId($planId)
    {
        $this->planId = $planId;
    }

    public function setUserId($userId)
    {
        $this->userId = $userId;
    }

    public function setPeriod($period)
    {
        if ($period) {
            $this->period = PlanService::getPeriodKey($period);
        }
    }

    /**
     * 静态单用户配额检查：仅 count 已成交订单，不写 Redis。
     * 验证按钮和下单前置都用它，多次调用幂等。
     */
    public function checkLimitUseWithUser(): bool
    {
        $limit = (int) $this->coupon->limit_use_with_user;
        if ($limit <= 0) return true;

        $usedCount = Order::where('coupon_id', $this->coupon->id)
            ->where('user_id', $this->userId)
            ->whereNotIn('status', [0, 2])
            ->count();
        return $usedCount < $limit;
    }

    /**
     * 占用单用户配额（仅下单时调用）。
     *
     * 原实现把 Redis INCR 占位塞在 check() 里，导致「验证」按钮也会占位，
     * 用户验证后再付款时第二次 check 直接踩到上限 → 报"已达上限"，付不了款。
     *
     * 现在拆出来：check() 只做静态校验；占位放到 use() 真正下单时进行。
     * 失败由 use() 或外部统一调 releaseUserQuota() 回滚。
     */
    private function acquireUserQuota(): bool
    {
        $limit = (int) $this->coupon->limit_use_with_user;
        if ($limit <= 0 || !$this->userId) return true;

        $usedCount = Order::where('coupon_id', $this->coupon->id)
            ->where('user_id', $this->userId)
            ->whereNotIn('status', [0, 2])
            ->count();
        if ($usedCount >= $limit) {
            return false;
        }

        $remain = $limit - $usedCount;
        $key = 'coupon:user_quota:' . $this->coupon->id . ':' . $this->userId;
        $current = (int) Redis::incr($key);
        if ($current === 1) {
            Redis::expire($key, 1800);
        }
        if ($current > $remain) {
            Redis::decr($key);
            return false;
        }
        return true;
    }

    /**
     * 回滚 acquireUserQuota 的占位（仅 use() 后续步骤失败时调）。
     */
    private function releaseUserQuota(): void
    {
        if ((int) $this->coupon->limit_use_with_user <= 0 || !$this->userId) {
            return;
        }
        $key = 'coupon:user_quota:' . $this->coupon->id . ':' . $this->userId;
        Redis::decr($key);
    }

    public function check()
    {
        if (!$this->coupon || !$this->coupon->show) {
            throw new ApiException(__('Invalid coupon'));
        }
        if ($this->coupon->limit_use <= 0 && $this->coupon->limit_use !== NULL) {
            throw new ApiException(__('This coupon is no longer available'));
        }
        if (time() < $this->coupon->started_at) {
            throw new ApiException(__('This coupon has not yet started'));
        }
        if (time() > $this->coupon->ended_at) {
            throw new ApiException(__('This coupon has expired'));
        }
        if ($this->coupon->limit_plan_ids && $this->planId) {
            if (!in_array($this->planId, $this->coupon->limit_plan_ids)) {
                throw new ApiException(__('The coupon code cannot be used for this subscription'));
            }
        }
        if ($this->coupon->limit_period && $this->period) {
            if (!in_array($this->period, $this->coupon->limit_period)) {
                throw new ApiException(__('The coupon code cannot be used for this period'));
            }
        }
        if ($this->coupon->limit_use_with_user !== NULL && $this->userId) {
            if (!$this->checkLimitUseWithUser()) {
                throw new ApiException(__('The coupon can only be used :limit_use_with_user per person', [
                    'limit_use_with_user' => $this->coupon->limit_use_with_user
                ]));
            }
        }
    }
}
