<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        if (!Schema::hasColumn('v2_server_machine', 'host')) {
            Schema::table('v2_server_machine', function (Blueprint $table) {
                $table->string('host')->nullable()->after('name');
                $table->unsignedInteger('ssh_port')->default(22)->after('host');
                $table->string('ssh_user', 64)->default('root')->after('ssh_port');
                $table->string('v2bx_status', 32)->nullable()->after('load_status');
                $table->string('v2bx_version', 32)->nullable()->after('v2bx_status');
            });
        }

        if (!Schema::hasTable('v2_server_machine_task')) {
            Schema::create('v2_server_machine_task', function (Blueprint $table) {
                $table->id();
                $table->string('batch', 36)->nullable()->index();
                $table->unsignedBigInteger('machine_id')->nullable()->index();
                $table->string('machine_name')->default('');
                $table->string('type', 32);
                $table->string('status', 16)->default('pending');
                $table->string('step', 64)->default('');
                $table->string('message', 512)->default('');
                $table->mediumText('log')->nullable();
                $table->json('params')->nullable();
                $table->unsignedInteger('started_at')->nullable();
                $table->unsignedInteger('finished_at')->nullable();
                $table->timestamps();

                $table->foreign('machine_id')->references('id')->on('v2_server_machine')->nullOnDelete();
            });
        }
    }

    public function down(): void
    {
        Schema::dropIfExists('v2_server_machine_task');
        if (Schema::hasColumn('v2_server_machine', 'host')) {
            Schema::table('v2_server_machine', function (Blueprint $table) {
                $table->dropColumn(['host', 'ssh_port', 'ssh_user', 'v2bx_status', 'v2bx_version']);
            });
        }
    }
};
