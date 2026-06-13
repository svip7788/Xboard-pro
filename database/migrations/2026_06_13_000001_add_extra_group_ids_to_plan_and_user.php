<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::table('v2_plan', function (Blueprint $table) {
            if (!Schema::hasColumn('v2_plan', 'extra_group_ids')) {
                $table->json('extra_group_ids')->nullable()->after('group_id')
                    ->comment('附加权限组ID列表（与 group_id 并列生效）');
            }
        });

        Schema::table('v2_user', function (Blueprint $table) {
            if (!Schema::hasColumn('v2_user', 'extra_group_ids')) {
                $table->json('extra_group_ids')->nullable()->after('group_id')
                    ->comment('附加权限组ID列表（与 group_id 并列生效）');
            }
        });
    }

    public function down(): void
    {
        Schema::table('v2_plan', function (Blueprint $table) {
            if (Schema::hasColumn('v2_plan', 'extra_group_ids')) {
                $table->dropColumn('extra_group_ids');
            }
        });
        Schema::table('v2_user', function (Blueprint $table) {
            if (Schema::hasColumn('v2_user', 'extra_group_ids')) {
                $table->dropColumn('extra_group_ids');
            }
        });
    }
};
