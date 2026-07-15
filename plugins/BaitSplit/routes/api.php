<?php

use Illuminate\Support\Facades\Route;
use Plugin\BaitSplit\Controllers\AdminController;

$securePath = admin_setting(
    'secure_path',
    admin_setting('frontend_admin_path', hash('crc32b', config('app.key')))
);

Route::prefix("api/v2/{$securePath}/plugin/bait-split")
    ->middleware('admin')
    ->group(function (): void {
        Route::get('/meta', [AdminController::class, 'meta']);
        Route::get('/status', [AdminController::class, 'status']);
        Route::post('/config', [AdminController::class, 'saveConfig']);
        Route::post('/start', [AdminController::class, 'start']);
        Route::post('/result', [AdminController::class, 'result']);
        Route::post('/disable', [AdminController::class, 'disable']);
    });
