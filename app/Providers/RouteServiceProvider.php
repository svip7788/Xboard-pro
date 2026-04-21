<?php

namespace App\Providers;

use Illuminate\Cache\RateLimiting\Limit;
use Illuminate\Foundation\Support\Providers\RouteServiceProvider as ServiceProvider;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\RateLimiter;
use Illuminate\Support\Facades\Route;

class RouteServiceProvider extends ServiceProvider
{
    /**
     * This namespace is applied to your controller routes.
     *
     * In addition, it is set as the URL generator's root namespace.
     *
     * @var string
     */
    protected $namespace = 'App\Http\Controllers';

    /**
     * Define your route model bindings, pattern filters, etc.
     *
     * @return void
     */
    public function boot()
    {
        // HTTPS scheme is forced per-request via middleware (Octane-safe).
        $this->configureRateLimiters();
        parent::boot();
    }

    /**
     * 针对无需登录的敏感接口配置限流：
     *   - passport.login:  每 IP 每分钟 10 次 + 每 email 每分钟 5 次
     *   - passport.forget: 每 IP 每分钟 5 次  + 每 email 每分钟 3 次
     *   - passport.send:   每 IP 每分钟 10 次 + 每 email 每分钟 1 次（邮件验证码）
     *   - passport.basic:  其他注册/令牌登录等，每 IP 每分钟 20 次
     *
     * 历史原因 Kernel.php 里把 'api' 分组的 throttle 注释掉了，导致登录/验证码/忘密
     * 全部无限流，可被爆破密码、轰炸邮箱、撞 6 位验证码。
     */
    protected function configureRateLimiters(): void
    {
        RateLimiter::for('passport.login', function (Request $request) {
            $email = strtolower((string) $request->input('email', ''));
            return [
                Limit::perMinute(10)->by('ip:' . $request->ip()),
                Limit::perMinute(5)->by('login-email:' . $email),
            ];
        });

        RateLimiter::for('passport.forget', function (Request $request) {
            $email = strtolower((string) $request->input('email', ''));
            return [
                Limit::perMinute(5)->by('ip:' . $request->ip()),
                Limit::perMinute(3)->by('forget-email:' . $email),
            ];
        });

        RateLimiter::for('passport.send', function (Request $request) {
            $email = strtolower((string) $request->input('email', ''));
            return [
                Limit::perMinute(10)->by('ip:' . $request->ip()),
                // Controller 里已有 60s 冷却，这里再设一个保险阈值
                Limit::perMinute(1)->by('send-email:' . $email),
            ];
        });

        RateLimiter::for('passport.basic', function (Request $request) {
            return Limit::perMinute(20)->by('ip:' . $request->ip());
        });
    }

    /**
     * Define the routes for the application.
     *
     * @return void
     */
    public function map()
    {
        $this->mapApiRoutes();
        $this->mapWebRoutes();

        //
    }

    /**
     * Define the "web" routes for the application.
     *
     * These routes all receive session state, CSRF protection, etc.
     *
     * @return void
     */
    protected function mapWebRoutes()
    {
        Route::middleware('web')
            ->namespace($this->namespace)
            ->group(base_path('routes/web.php'));
    }

    /**
     * Define the "api" routes for the application.
     *
     * These routes are typically stateless.
     *
     * @return void
     */
    protected function mapApiRoutes()
    {
        Route::group([
            'prefix' => '/api/v1',
            'middleware' => 'api',
            'namespace' => $this->namespace
        ], function ($router) {
            foreach (glob(app_path('Http//Routes//V1') . '/*.php') as $file) {
                $this->app->make('App\\Http\\Routes\\V1\\' . basename($file, '.php'))->map($router);
            }
        });


        Route::group([
            'prefix' => '/api/v2',
            'middleware' => 'api',
            'namespace' => $this->namespace
        ], function ($router) {
            foreach (glob(app_path('Http//Routes//V2') . '/*.php') as $file) {
                $this->app->make('App\\Http\\Routes\\V2\\' . basename($file, '.php'))->map($router);
            }
        });
    }
}
