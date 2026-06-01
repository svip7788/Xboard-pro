<?php

namespace App\Http\Controllers\V1\User;

use App\Http\Controllers\Controller;
use App\Http\Requests\User\UserChangePassword;
use App\Http\Requests\User\UserTransfer;
use App\Http\Requests\User\UserUpdate;
use App\Models\Order;
use App\Models\Plan;
use App\Models\Ticket;
use App\Models\User;
use App\Services\Auth\LoginService;
use App\Services\AuthService;
use App\Services\Plugin\HookManager;
use App\Services\UserService;
use App\Utils\CacheKey;
use App\Utils\Helper;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;

class UserController extends Controller
{
    protected $loginService;

    public function __construct(
        LoginService $loginService
    ) {
        $this->loginService = $loginService;
    }

    public function getActiveSession(Request $request)
    {
        $user = $request->user();
        $authService = new AuthService($user);
        return $this->success($authService->getSessions());
    }

    public function removeActiveSession(Request $request)
    {
        $user = $request->user();
        $authService = new AuthService($user);
        return $this->success($authService->removeSession($request->input('session_id')));
    }

    public function checkLogin(Request $request)
    {
        $data = [
            'is_login' => $request->user()?->id ? true : false
        ];
        if ($request->user()?->is_admin) {
            $data['is_admin'] = true;
        }
        return $this->success($data);
    }

    public function changePassword(UserChangePassword $request)
    {
        $user = $request->user();
        if (
            !Helper::multiPasswordVerify(
                $user->password_algo,
                $user->password_salt,
                $request->input('old_password'),
                $user->password
            )
        ) {
            return $this->fail([400, __('The old password is wrong')]);
        }
        $user->password = password_hash($request->input('new_password'), PASSWORD_DEFAULT);
        $user->password_algo = NULL;
        $user->password_salt = NULL;
        if (!$user->save()) {
            return $this->fail([400, __('Save failed')]);
        }
        
        $currentToken = $user->currentAccessToken();
        if ($currentToken) {
            $user->tokens()->where('id', '!=', $currentToken->id)->delete();
        } else {
            $user->tokens()->delete();
        }
        
        return $this->success(true);
    }

    public function info(Request $request)
    {
        $user = User::where('id', $request->user()->id)
            ->select([
                'email',
                'transfer_enable',
                'last_login_at',
                'created_at',
                'banned',
                'remind_expire',
                'remind_traffic',
                'expired_at',
                'balance',
                'commission_balance',
                'plan_id',
                'discount',
                'commission_rate',
                'telegram_id',
                'uuid'
            ])
            ->first();
        if (!$user) {
            return $this->fail([400, __('The user does not exist')]);
        }
        $user['avatar_url'] = 'https://cdn.v2ex.com/gravatar/' . md5($user->email) . '?s=64&d=identicon';
        return $this->success($user);
    }

    public function getStat(Request $request)
    {
        $stat = [
            Order::where('status', 0)
                ->where('user_id', $request->user()->id)
                ->count(),
            Ticket::where('status', 0)
                ->where('user_id', $request->user()->id)
                ->count(),
            User::where('invite_user_id', $request->user()->id)
                ->count()
        ];
        return $this->success($stat);
    }

    public function getSubscribe(Request $request)
    {
        $user = User::where('id', $request->user()->id)
            ->select([
                'plan_id',
                'token',
                'expired_at',
                'u',
                'd',
                'transfer_enable',
                'email',
                'uuid',
                'device_limit',
                'speed_limit',
                'next_reset_at'
            ])
            ->first();
        if (!$user) {
            return $this->fail([400, __('The user does not exist')]);
        }
        if ($user->plan_id) {
            $user['plan'] = Plan::find($user->plan_id);
            if (!$user['plan']) {
                return $this->fail([400, __('Subscription plan does not exist')]);
            }
        }
        $user['subscribe_url'] = Helper::getSubscribeUrl($user['token']);
        $userService = new UserService();
        $user['reset_day'] = $userService->getResetDay($user);
        $user = HookManager::filter('user.subscribe.response', $user);
        return $this->success($user);
    }

    public function resetSecurity(Request $request)
    {
        $user = $request->user();
        $user->uuid = Helper::guid(true);
        $user->token = Helper::guid();
        if (!$user->save()) {
            return $this->fail([400, __('Reset failed')]);
        }
        return $this->success(Helper::getSubscribeUrl($user->token));
    }

    public function update(UserUpdate $request)
    {
        $updateData = $request->only([
            'remind_expire',
            'remind_traffic'
        ]);

        $user = $request->user();
        try {
            $user->update($updateData);
        } catch (\Exception $e) {
            return $this->fail([400, __('Save failed')]);
        }

        return $this->success(true);
    }

    public function transfer(UserTransfer $request)
    {
        $amount = $request->input('transfer_amount');
        try {
            DB::transaction(function () use ($request, $amount) {
                $user = User::lockForUpdate()->find($request->user()->id);
                if (!$user) {
                    throw new \Exception(__('The user does not exist'));
                }
                if ($amount > $user->commission_balance) {
                    throw new \Exception(__('Insufficient commission balance'));
                }
                $user->commission_balance -= $amount;
                $user->balance += $amount;
                if (!$user->save()) {
                    throw new \Exception(__('Transfer failed'));
                }
            });
        } catch (\Exception $e) {
            return $this->fail([400, $e->getMessage()]);
        }
        return $this->success(true);
    }

    public function getQuickLoginUrl(Request $request)
    {
        $user = $request->user();

        $url = $this->loginService->generateQuickLoginUrl($user, $request->input('redirect'));
        return $this->success($url);
    }

    /**
     * 获取订阅刷新解锁窗口时长（分钟），从 admin_setting 读取，clamp 到 [1, 1440]。
     */
    private function getSubscribeUnlockMinutes(): int
    {
        $minutes = (int) admin_setting('subscribe_refresh_lock_duration_minutes', 10);
        if ($minutes < 1) {
            $minutes = 1;
        } elseif ($minutes > 1440) {
            $minutes = 1440;
        }
        return $minutes;
    }

    /**
     * 获取每日解锁次数上限，0 或 null 表示无限。
     */
    private function getSubscribeUnlockDailyLimit(): ?int
    {
        $raw = admin_setting('subscribe_refresh_lock_daily_limit');
        if ($raw === null || $raw === '' || (int) $raw <= 0) {
            return null;
        }
        return min((int) $raw, 1000);
    }

    /**
     * 获取每日次数计数器 Redis 键 + 当天剩余秒数。
     *
     * @return array{0:string,1:int}
     */
    private function getSubscribeUnlockDailyCounter(int $userId): array
    {
        $date = date('Ymd');
        $key = CacheKey::get('SUBSCRIBE_UNLOCK_DAILY_COUNT', $userId . '_' . $date);
        $ttl = max(60, strtotime('tomorrow') - time());
        return [$key, $ttl];
    }

    /**
     * 临时解锁订阅刷新。
     */
    public function unlockSubscribe(Request $request)
    {
        $user = $request->user();
        $cacheKey = CacheKey::get('SUBSCRIBE_REFRESH_ALLOWED', $user->id);
        $existing = Cache::get($cacheKey);
        $now = time();
        $minutes = $this->getSubscribeUnlockMinutes();
        $limit = $this->getSubscribeUnlockDailyLimit();

        [$counterKey, $counterTtl] = $this->getSubscribeUnlockDailyCounter($user->id);
        $used = (int) Cache::get($counterKey, 0);

        // 已在解锁窗口内：续期不扣次数（幂等）
        if ($existing && (int) $existing > $now) {
            $expiresAt = $now + $minutes * 60;
            Cache::put($cacheKey, $expiresAt, $minutes * 60);
            return $this->success([
                'locked' => false,
                'expires_at' => $expiresAt,
                'duration_minutes' => $minutes,
                'daily_limit' => $limit,
                'daily_used' => $used,
                'daily_remaining' => $limit === null ? null : max(0, $limit - $used),
            ]);
        }

        // 新开窗口：先检查次数上限
        if ($limit !== null && $used >= $limit) {
            return $this->fail([429, __('Daily unlock limit reached, try again tomorrow')]);
        }

        $expiresAt = $now + $minutes * 60;
        Cache::put($cacheKey, $expiresAt, $minutes * 60);
        $newUsed = $used + 1;
        Cache::put($counterKey, $newUsed, $counterTtl);

        return $this->success([
            'locked' => false,
            'expires_at' => $expiresAt,
            'duration_minutes' => $minutes,
            'daily_limit' => $limit,
            'daily_used' => $newUsed,
            'daily_remaining' => $limit === null ? null : max(0, $limit - $newUsed),
        ]);
    }

    /**
     * 立刻锁定订阅刷新（用户主动取消窗口）。
     */
    public function lockSubscribe(Request $request)
    {
        $user = $request->user();
        Cache::forget(CacheKey::get('SUBSCRIBE_REFRESH_ALLOWED', $user->id));

        $limit = $this->getSubscribeUnlockDailyLimit();
        [$counterKey] = $this->getSubscribeUnlockDailyCounter($user->id);
        $used = (int) Cache::get($counterKey, 0);

        return $this->success([
            'locked' => true,
            'expires_at' => null,
            'remaining_seconds' => 0,
            'daily_limit' => $limit,
            'daily_used' => $used,
            'daily_remaining' => $limit === null ? null : max(0, $limit - $used),
        ]);
    }

    /**
     * 查询当前订阅锁定状态。
     */
    public function subscribeLockStatus(Request $request)
    {
        $user = $request->user();
        $featureEnabled = (bool) (int) admin_setting('subscribe_refresh_lock_enable', 0);
        $expiresAt = Cache::get(CacheKey::get('SUBSCRIBE_REFRESH_ALLOWED', $user->id));

        $now = time();
        $unlocked = $expiresAt && (int) $expiresAt > $now;

        $limit = $this->getSubscribeUnlockDailyLimit();
        [$counterKey] = $this->getSubscribeUnlockDailyCounter($user->id);
        $used = (int) Cache::get($counterKey, 0);

        return $this->success([
            'feature_enabled' => $featureEnabled,
            'locked' => !$unlocked,
            'expires_at' => $unlocked ? (int) $expiresAt : null,
            'remaining_seconds' => $unlocked ? (int) $expiresAt - $now : 0,
            'duration_minutes' => $this->getSubscribeUnlockMinutes(),
            'daily_limit' => $limit,
            'daily_used' => $used,
            'daily_remaining' => $limit === null ? null : max(0, $limit - $used),
        ]);
    }
}
