<?php

namespace App\Observers;

use App\Jobs\NodeUserSyncJob;
use App\Models\User;
use App\Services\TrafficResetService;

class UserObserver
{
  public bool $afterCommit = true;

  public function __construct(
    private readonly TrafficResetService $trafficResetService
  ) {
  }

  public function updated(User $user): void
  {
    // With $afterCommit = true, isDirty() is always false after commit.
    // Use wasChanged() to detect what was actually modified.
    $syncFields = ['group_id', 'extra_group_ids', 'uuid', 'speed_limit', 'device_limit', 'banned', 'expired_at', 'transfer_enable', 'u', 'd', 'plan_id'];
    $needsSync = $user->wasChanged($syncFields);

    $oldGroupId = $user->wasChanged('group_id') ? $user->getOriginal('group_id') : null;
    $oldExtraGroupIds = [];
    if ($user->wasChanged('extra_group_ids')) {
      $orig = $user->getOriginal('extra_group_ids');
      if (is_string($orig)) {
        $decoded = json_decode($orig, true);
        $oldExtraGroupIds = is_array($decoded) ? array_map('intval', $decoded) : [];
      } elseif (is_array($orig)) {
        $oldExtraGroupIds = array_map('intval', $orig);
      }
    }

    if ($user->wasChanged(['plan_id', 'expired_at'])) {
      $this->recalculateNextResetAt($user);
    }

    if ($needsSync) {
      NodeUserSyncJob::dispatch($user->id, 'updated', $oldGroupId, $oldExtraGroupIds);
    }
  }

  public function created(User $user): void
  {
    $this->recalculateNextResetAt($user);
    NodeUserSyncJob::dispatch($user->id, 'created');
  }

  public function deleted(User $user): void
  {
    $oldExtra = is_array($user->extra_group_ids) ? array_map('intval', $user->extra_group_ids) : [];
    if ($user->group_id || !empty($oldExtra)) {
      NodeUserSyncJob::dispatch($user->id, 'deleted', $user->group_id, $oldExtra);
    }
  }

  /**
   * 根据当前用户状态重新计算 next_reset_at
   */
  private function recalculateNextResetAt(User $user): void
  {
    $user->refresh();
    User::withoutEvents(function () use ($user) {
      $nextResetTime = $this->trafficResetService->calculateNextResetTime($user);
      $user->next_reset_at = $nextResetTime?->timestamp;
      $user->save();
    });
  }
}