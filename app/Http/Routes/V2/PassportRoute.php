<?php
namespace App\Http\Routes\V2;

use App\Http\Controllers\V1\Passport\AuthController;
use App\Http\Controllers\V1\Passport\CommController;
use Illuminate\Contracts\Routing\Registrar;

class PassportRoute
{
    public function map(Registrar $router)
    {
        $router->group([
            'prefix' => 'passport'
        ], function ($router) {
            // Auth
            $router->post('/auth/register', [AuthController::class, 'register'])
                ->middleware('throttle:passport.basic');
            $router->post('/auth/login', [AuthController::class, 'login'])
                ->middleware('throttle:passport.login');
            $router->get ('/auth/token2Login', [AuthController::class, 'token2Login'])
                ->middleware('throttle:passport.basic');
            $router->post('/auth/forget', [AuthController::class, 'forget'])
                ->middleware('throttle:passport.forget');
            $router->post('/auth/getQuickLoginUrl', [AuthController::class, 'getQuickLoginUrl'])
                ->middleware('throttle:passport.basic');
            $router->post('/auth/loginWithMailLink', [AuthController::class, 'loginWithMailLink'])
                ->middleware('throttle:passport.send');
            // Comm
            $router->post('/comm/sendEmailVerify', [CommController::class, 'sendEmailVerify'])
                ->middleware('throttle:passport.send');
            $router->post('/comm/pv', [CommController::class, 'pv']);
        });
    }
}
