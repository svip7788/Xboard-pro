<?php

/**
 * 凌晨收敛的回归用例。
 *
 * 这是现在唯一在生效的防护：墙压倒性落在凌晨，窗口内把普通用户挤到牺牲池，
 * 拿一两个 IP 换其余几个的存活。改错了会在半夜静默生效，所以必须有用例兜着。
 *
 * 跑法：php tests/night_test.php
 */

declare(strict_types=1);

require __DIR__ . '/bootstrap.php';

use Plugin\BaitSplit\Tests\RouterTestService;
use Plugin\BaitSplit\Tests\T;

/** 五个池：三个主组、两个牺牲池，另有一个停用的。 */
function router(): array
{
    $pools = [];
    foreach ([
        'p_cs1' => ['cs1', 'enabled'],
        'p_cs2' => ['cs2', 'enabled'],
        'p_cs3' => ['cs3', 'enabled'],
        'p_cs4' => ['cs4', 'enabled'],
        'p_cs5' => ['cs5', 'enabled'],
        'p_dead' => ['dead', 'blocked'],
    ] as $id => [$webhook, $status]) {
        $pools[$id] = [
            'id' => $id,
            'name' => $webhook,
            'webhook_id' => $webhook,
            'enabled' => true,
            'status' => $status === 'blocked' ? 'blocked' : 'active',
        ];
    }
    return ['pools' => $pools];
}

function svc(array $extra = []): RouterTestService
{
    return new RouterTestService(array_merge([
        'night_converge_enabled' => true,
        'night_converge_pool_ids' => 'cs4,cs5',
        'night_converge_start' => 1,
        'night_converge_end' => 9,
    ], $extra));
}

/** nightConvergePoolIds 是私有的，且不吃引用，用 callValue 打进去。 */
function converge(RouterTestService $s, ?array $override = null, int $uid = 2): array
{
    return $s->callValue(
        'nightConvergePoolIds',
        router(),
        ['p_cs1', 'p_cs2'],
        $override,
        $uid
    );
}

$hour = (int) date('G');
$inWindow = $hour >= 1 && $hour < 9;

T::case('开关关掉时原样返回');
T::same(
    ['p_cs1', 'p_cs2'],
    converge(svc(['night_converge_enabled' => false])),
    '不收敛'
);

T::case('牺牲池没填时原样返回');
T::same(
    ['p_cs1', 'p_cs2'],
    converge(svc(['night_converge_pool_ids' => ''])),
    '留空不生效'
);

T::case('牺牲池全不可用时原样返回');
T::same(
    ['p_cs1', 'p_cs2'],
    converge(svc(['night_converge_pool_ids' => 'dead'])),
    '停用的池不能当牺牲池'
);

T::case('人工锁定的用户不参与');
T::same(
    ['p_cs1', 'p_cs2'],
    converge(svc(['night_converge_start' => 0, 'night_converge_end' => 24]),
        ['pool_id' => 'p_cs1']),
    '有 pool_id 覆盖就跳过'
);

T::case('窗口内牺牲池插到最前');
$full = svc(['night_converge_start' => 0, 'night_converge_end' => 24]);
$got = converge($full, null, 2);
T::same('p_cs4', $got[0] ?? '', 'uid=2 落 cs4（2 % 2 = 0）');
T::same('p_cs5', converge($full, null, 3)[0] ?? '', 'uid=3 落 cs5（3 % 2 = 1）');
T::ok(
    in_array('p_cs1', $got, true) && in_array('p_cs2', $got, true),
    '原有池仍然保留在后面兜底'
);

T::case('同一个人反复拉订阅落同一个池');
$seen = [];
for ($i = 0; $i < 5; $i++) {
    $seen[] = converge($full, null, 12345)[0] ?? '';
}
T::same(1, count(array_unique($seen)), '五次结果一致，窗口内不会来回换地址');

T::case('窗口边界');
$win = fn(int $start, int $end): bool => (bool) svc([
    'night_converge_start' => $start,
    'night_converge_end' => $end,
])->callValue('inNightConvergeWindow');
T::ok($win(0, 24), '0-24 全天命中');
T::same(false, $win(5, 5), '起止相同视为未配置');
T::same(false, $win(-1, 9), '越界视为未配置');
T::same(false, $win(1, 25), '越界视为未配置');
T::same(
    $hour >= 22 || $hour < 2,
    $win(22, 2),
    '跨零点窗口按当前 ' . $hour . ' 点判定'
);
T::same($inWindow, $win(1, 9), '默认 1-9 窗口与当前时间一致');

T::case('线上默认配置在窗口内外的实际表现');
$live = svc();
T::same(
    $inWindow,
    ($converged = converge($live, null, 2)) !== ['p_cs1', 'p_cs2'],
    '当前 ' . $hour . ' 点' . ($inWindow ? '应当收敛' : '应当不收敛')
);
if ($inWindow) {
    T::same('p_cs4', $converged[0] ?? '', '窗口内落牺牲池');
}

exit(T::summary());
