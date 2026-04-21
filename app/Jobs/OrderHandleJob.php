<?php

namespace App\Jobs;

use App\Models\Order;
use App\Services\OrderService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;

class OrderHandleJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;
    protected $order;
    protected $tradeNo;

    public $tries = 3;
    /**
     * OrderService::open() 里含多条 DB 写和锁，原来的 5 秒极易超时，
     * 超时会被 Horizon 当失败重试 → 同一订单被重复 open → 用户被多开一次套餐。
     */
    public $timeout = 60;
    /**
     * Create a new job instance.
     *
     * @return void
     */
    public function __construct($tradeNo)
    {
        $this->onQueue('order_handle');
        $this->tradeNo = $tradeNo;
    }

    /**
     * Execute the job.
     *
     * @return void
     */
    public function handle()
    {
        // 把 lockForUpdate 和后续动作全部包进同一个事务，
        // 否则 lockForUpdate 取到的锁在返回查询结果那一刻就释放了，并发重试照样双开。
        DB::transaction(function () {
            $order = Order::where('trade_no', $this->tradeNo)
                ->lockForUpdate()
                ->first();
            if (!$order) return;
            $orderService = new OrderService($order);
            switch ($order->status) {
                case Order::STATUS_PENDING:
                    if ($order->created_at <= (time() - 3600 * 2)) {
                        $orderService->cancel();
                    }
                    break;
                case Order::STATUS_PROCESSING:
                    $orderService->open();
                    break;
            }
        });
    }
}
