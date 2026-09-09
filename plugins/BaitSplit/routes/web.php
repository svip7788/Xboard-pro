<?php

use Illuminate\Support\Facades\Route;

$securePath = admin_setting(
    'secure_path',
    admin_setting('frontend_admin_path', hash('crc32b', config('app.key')))
);

// 原路径（带 securePath）
Route::get(
    "/{$securePath}/plugins/bait-split/console",
    fn() => view('BaitSplit::console', [
        'apiBase' => "/api/v2/{$securePath}/plugin/bait-split",
        'adminUrl' => "/{$securePath}",
    ])
);

// SPA 跳转路径（不带 securePath）- 重定向到正确地址
Route::get('/plugins/bait_split/console', function () use ($securePath) {
    return redirect("/{$securePath}/plugins/bait-split/console");
});
