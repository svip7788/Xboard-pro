<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('v2_bait_split_wall_events', function (Blueprint $table) {
            $table->id();
            $table->string('campaign_id', 64)->index();
            $table->unsignedInteger('event_at')->index();
            $table->string('reason', 20)->default('blocked');
            $table->string('old_ip', 253)->nullable();
            $table->string('new_ip', 253);
            $table->json('pool_ids');
            $table->json('pool_names');
            $table->json('suspect_user_ids');
            $table->json('exact_user_ids');
            $table->unsignedInteger('suspect_count')->default(0);
            $table->unsignedInteger('exact_count')->default(0);
            $table->timestamp('created_at')->useCurrent();
            
            $table->index(['campaign_id', 'event_at']);
            $table->index(['campaign_id', 'reason']);
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('v2_bait_split_wall_events');
    }
};
