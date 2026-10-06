<?php

namespace App\Http\Controllers\V1\User;

use App\Http\Controllers\Controller;
use App\Models\User;
use App\Services\TrafficExchangeService;
use Illuminate\Http\Request;
use Illuminate\Validation\Rule;

class TrafficExchangeController extends Controller
{
    public function __construct(
        private readonly TrafficExchangeService $trafficExchangeService
    ) {
    }

    public function options(Request $request)
    {
        $user = User::findOrFail($request->user()->id);
        return $this->success($this->trafficExchangeService->getOptions($user));
    }

    public function exchange(Request $request)
    {
        $request->validate([
            'mode' => ['required', Rule::in(TrafficExchangeService::MODES)],
        ]);

        $user = User::findOrFail($request->user()->id);
        return $this->success($this->trafficExchangeService->exchange($user, $request->input('mode')));
    }
}
