<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    private const INDEX = 'idx_plan_id_expired_at';

    public function up(): void
    {
        if (Schema::hasIndex('v2_user', self::INDEX)) {
            return;
        }
        // 套餐列表按 plan_id 统计用户数 / 有效用户数（expired_at）
        Schema::table('v2_user', function (Blueprint $table) {
            $table->index(['plan_id', 'expired_at'], self::INDEX);
        });
    }

    public function down(): void
    {
        if (!Schema::hasIndex('v2_user', self::INDEX)) {
            return;
        }
        Schema::table('v2_user', function (Blueprint $table) {
            $table->dropIndex(self::INDEX);
        });
    }
};
