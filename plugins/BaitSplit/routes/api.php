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
        Route::post('/ping', [AdminController::class, 'createPing']);
        Route::get('/ping/{taskId}', [AdminController::class, 'pingResult']);
        Route::get('/campaigns', [AdminController::class, 'campaigns']);
        Route::get('/campaigns/{campaignId}/exposures', [AdminController::class, 'exposures']);
        Route::post('/campaigns', [AdminController::class, 'saveCampaign']);
        Route::delete('/campaigns/{campaignId}', [AdminController::class, 'deleteCampaign']);
        Route::post('/campaigns/{campaignId}/start', [AdminController::class, 'start']);
        Route::post('/campaigns/{campaignId}/replace-domains', [AdminController::class, 'replaceDomains']);
        Route::post('/campaigns/{campaignId}/result', [AdminController::class, 'result']);
        Route::post('/campaigns/{campaignId}/disable', [AdminController::class, 'disable']);
        Route::post('/campaigns/{campaignId}/reset', [AdminController::class, 'reset']);
        Route::post('/campaigns/{campaignId}/router/initialize', [AdminController::class, 'initializeRouter']);
        Route::post('/campaigns/{campaignId}/router/toggle', [AdminController::class, 'toggleRouter']);
        Route::post('/campaigns/{campaignId}/router/sync-users', [AdminController::class, 'syncRouterUsers']);
        Route::post('/campaigns/{campaignId}/router/rollback', [AdminController::class, 'rollbackRouter']);
        Route::post('/campaigns/{campaignId}/pools', [AdminController::class, 'savePool']);
        Route::delete('/campaigns/{campaignId}/pools/{poolId}', [AdminController::class, 'deletePool']);
        Route::get('/campaigns/{campaignId}/pools/{poolId}/users', [AdminController::class, 'poolUsers']);
        Route::post('/campaigns/{campaignId}/pools/{poolId}/move-pulled', [AdminController::class, 'movePulledPoolUsers']);
        Route::post('/campaigns/{campaignId}/pools/{poolId}/investigation', [AdminController::class, 'createInvestigationRoot']);
        Route::post('/campaigns/{campaignId}/investigations/{nodeId}/split', [AdminController::class, 'splitInvestigationNode']);
        Route::post('/campaigns/{campaignId}/investigations/{nodeId}/status', [AdminController::class, 'setInvestigationNodeStatus']);
        Route::delete('/campaigns/{campaignId}/investigations/{nodeId}', [AdminController::class, 'deleteInvestigationTree']);
        Route::post('/campaigns/{campaignId}/investigations/{nodeId}/move', [AdminController::class, 'moveInvestigationNodeUsers']);
        Route::get('/campaigns/{campaignId}/investigations/{nodeId}/users', [AdminController::class, 'investigationNodeUsers']);
        Route::get('/campaigns/{campaignId}/users/search', [AdminController::class, 'searchUsers']);
        Route::get('/campaigns/{campaignId}/overrides', [AdminController::class, 'overrides']);
        Route::post('/campaigns/{campaignId}/overrides/{userId}', [AdminController::class, 'saveOverride']);
        Route::delete('/campaigns/{campaignId}/overrides/{userId}', [AdminController::class, 'deleteOverride']);
    });
