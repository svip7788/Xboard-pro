<?php

use Illuminate\Support\Facades\Route;

$securePath = admin_setting(
    'secure_path',
    admin_setting('frontend_admin_path', hash('crc32b', config('app.key')))
);

Route::get(
    "/{$securePath}/plugins/bait-split/console",
    fn() => view('BaitSplit::console', [
        'apiBase' => "/api/v2/{$securePath}/plugin/bait-split",
        'adminUrl' => "/{$securePath}",
    ])
);
