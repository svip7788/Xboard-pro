<?php

namespace App\Http\Controllers\V2\Admin;

use App\Http\Controllers\Controller;
use App\Services\UpdateService;
use Illuminate\Http\Request;

class UpdateController extends Controller
{
    protected $updateService;

    public function __construct(UpdateService $updateService)
    {
        $this->updateService = $updateService;
    }

    public function checkUpdate(Request $request)
    {
        return $this->success($this->updateService->checkForUpdates($request->boolean('force')));
    }

    public function executeUpdate()
    {
        $result = $this->updateService->startUpdate();
        return $result['success'] ? $this->success($result) : $this->fail([500, $result['message']]);
    }

    public function status()
    {
        return $this->success($this->updateService->getUpdateStatus());
    }
}
