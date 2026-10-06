<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::table('v2_order', function (Blueprint $table) {
            if (!Schema::hasColumn('v2_order', 'restart')) {
                $table->boolean('restart')->default(false)->after('type')
                    ->comment('续费立即生效：到期日从开通时重新计算并重置流量');
            }
        });
    }

    public function down(): void
    {
        Schema::table('v2_order', function (Blueprint $table) {
            if (Schema::hasColumn('v2_order', 'restart')) {
                $table->dropColumn('restart');
            }
        });
    }
};
