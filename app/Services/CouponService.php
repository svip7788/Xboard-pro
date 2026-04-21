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
        $this->check();
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
     * 单用户使用次数配额检查。
     *
     * 原实现为 count() + 后续 use() 两步非原子,高并发下同一用户可突破个人配额。
     * 这里在 count() 之上再叠加一个 Redis 原子计数:
     *   - 用 INCR 取得"当前占位数",满额则立刻 DECR 归还并拒绝;
     *   - 订单正常创建 → 占位落入实际 Order 表,Redis 计数会在 TTL(30min)后
     *     自动衰减,由后续真实订单 count() 兜底,不会长期漂移。
     *
     * 这不是强一致方案,但把"同一用户秒级并发超用"收敛到 Redis 原子粒度,足够对抗
     * 普通脚本刷券。真正的强一致要落在 DB 唯一索引,超出本次修复范围。
     */
    public function checkLimitUseWithUser(): bool
    {
        $limit = (int) $this->coupon->limit_use_with_user;
        if ($limit <= 0) return true;

        $usedCount = Order::where('coupon_id', $this->coupon->id)
            ->where('user_id', $this->userId)
            ->whereNotIn('status', [0, 2])
            ->count();
        if ($usedCount >= $limit) {
            return false;
        }

        $remain = $limit - $usedCount;
        $key = 'coupon:user_quota:' . $this->coupon->id . ':' . $this->userId;
        // INCR 拿到占位后的总数
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
