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
        Route::get('/campaigns', [AdminController::class, 'campaigns']);
        Route::get('/campaigns/{campaignId}/exposures', [AdminController::class, 'exposures']);
        Route::post('/campaigns', [AdminController::class, 'saveCampaign']);
        Route::delete('/campaigns/{campaignId}', [AdminController::class, 'deleteCampaign']);
        Route::post('/campaigns/{campaignId}/start', [AdminController::class, 'start']);
        Route::post('/campaigns/{campaignId}/replace-domains', [AdminController::class, 'replaceDomains']);
        Route::post('/campaigns/{campaignId}/result', [AdminController::class, 'result']);
        Route::post('/campaigns/{campaignId}/disable', [AdminController::class, 'disable']);
        Route::post('/campaigns/{campaignId}/reset', [AdminController::class, 'reset']);
    });
