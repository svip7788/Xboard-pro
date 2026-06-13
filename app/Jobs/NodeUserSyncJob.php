<?php

namespace App\Jobs;

use App\Models\User;
use App\Services\NodeSyncService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;

class NodeUserSyncJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public $tries = 2;
    public $timeout = 10;

    public function __construct(
        private readonly int $userId,
        private readonly string $action,
        private readonly ?int $oldGroupId = null,
        private readonly array $oldExtraGroupIds = []
    ) {
        $this->onQueue('node_sync');
    }

    public function handle(): void
    {
        $user = User::find($this->userId);

        // 老组（主 + 附加）都广播一次 remove，避免用户切套餐后仍能从老组节点查到
        $oldGroups = [];
        if ($this->oldGroupId) {
            $oldGroups[] = (int) $this->oldGroupId;
        }
        foreach ($this->oldExtraGroupIds as $gid) {
            if ((int) $gid > 0) {
                $oldGroups[] = (int) $gid;
            }
        }
        $oldGroups = array_values(array_unique($oldGroups));

        if ($this->action === 'updated' || $this->action === 'created') {
            foreach ($oldGroups as $gid) {
                NodeSyncService::notifyUserRemovedFromGroup($this->userId, $gid);
            }
            if ($user) {
                NodeSyncService::notifyUserChanged($user);
            }
        } elseif ($this->action === 'deleted') {
            foreach ($oldGroups as $gid) {
                NodeSyncService::notifyUserRemovedFromGroup($this->userId, $gid);
            }
        }
    }
}
