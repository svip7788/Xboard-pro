<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        if (!Schema::hasColumn('v2_server_machine', 'ssh_password')) {
            Schema::table('v2_server_machine', function (Blueprint $table) {
                $table->text('ssh_password')->nullable()->after('ssh_user');
                $table->text('ssh_key')->nullable()->after('ssh_password');
            });
        }
    }

    public function down(): void
    {
        if (Schema::hasColumn('v2_server_machine', 'ssh_password')) {
            Schema::table('v2_server_machine', function (Blueprint $table) {
                $table->dropColumn(['ssh_password', 'ssh_key']);
            });
        }
    }
};
