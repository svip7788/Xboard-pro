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

/** 两个方法都是私有的，用 callValue 打进去。 */
function targets(RouterTestService $s, ?array $override = null): array
{
    return $s->callValue('nightConvergeTargets', router(), $override);
}

function pick(RouterTestService $s, array $t, int $uid, int $serverId): string
{
    return $s->callValue('convergeTargetForServer', $t, $uid, $serverId);
}

$hour = (int) date('G');
$inWindow = $hour >= 1 && $hour < 9;
$full = svc(['night_converge_start' => 0, 'night_converge_end' => 24]);

T::case('不该收敛的情形一律返回空');
T::same([], targets(svc(['night_converge_enabled' => false])), '开关关掉');
T::same([], targets(svc(['night_converge_pool_ids' => ''])), '牺牲池留空');
T::same([], targets(svc(['night_converge_pool_ids' => 'dead'])), '牺牲池已停用');
T::same([], targets($full, ['pool_id' => 'p_cs1']), '人工锁定的用户不参与');

T::case('窗口内返回全部可用牺牲池');
T::same(['p_cs4', 'p_cs5'], targets($full), '两个池都要给出来，供按节点分摊');
T::same(
    ['p_cs5'],
    targets(svc([
        'night_converge_pool_ids' => 'dead,cs5',
        'night_converge_start' => 0,
        'night_converge_end' => 24,
    ])),
    '停用的池被过滤掉，可用的留下'
);

T::case('同一个人的节点分散到两个牺牲池');
$t = ['p_cs4', 'p_cs5'];
$spread = [];
for ($serverId = 1; $serverId <= 50; $serverId++) {
    $spread[pick($full, $t, 2367, $serverId)][] = $serverId;
}
T::same(2, count($spread), '50 个节点落在两个地址上，一个被墙另一半还活着');
T::ok(
    count($spread['p_cs4']) >= 20 && count($spread['p_cs5']) >= 20,
    '两边大致均分：cs4 拿 ' . count($spread['p_cs4']) . ' 个，cs5 拿 ' . count($spread['p_cs5']) . ' 个'
);

T::case('同一个人同一个节点的落点是定死的');
$seen = [];
for ($i = 0; $i < 5; $i++) {
    $seen[] = pick($full, $t, 12345, 77);
}
T::same(1, count(array_unique($seen)), '反复拉订阅不会来回换地址');

T::case('不同用户的同一个节点不会全压在一个地址上');
$byUser = [];
for ($uid = 1; $uid <= 40; $uid++) {
    $byUser[pick($full, $t, $uid, 77)][] = $uid;
}
T::same(2, count($byUser), '同一个节点在不同用户之间也分摊');

T::case('只配一个牺牲池时退化为单地址');
$one = ['p_cs5'];
$all = [];
for ($serverId = 1; $serverId <= 10; $serverId++) {
    $all[] = pick($full, $one, 2367, $serverId);
}
T::same(['p_cs5'], array_values(array_unique($all)), '全部落同一个池，不报错');

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
T::same($inWindow, $win(1, 9), '1-9 窗口与当前时间一致');

T::case('线上配置（0-10 点）在当前时刻的实际表现');
$live = svc(['night_converge_start' => 0, 'night_converge_end' => 10]);
$nowIn = $hour >= 0 && $hour < 10;
T::same(
    $nowIn ? ['p_cs4', 'p_cs5'] : [],
    targets($live),
    '当前 ' . $hour . ' 点' . ($nowIn ? '应当收敛' : '应当不收敛')
);

exit(T::summary());
