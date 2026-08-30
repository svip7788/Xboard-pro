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

use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Redis;
use Plugin\BaitSplit\Tests\RouterTestService;
use Plugin\BaitSplit\Tests\T;

/**
 * 五个池：三个主组、两个牺牲池，另有一个停用的。
 *
 * 归属表定死：偶数 uid 归牺牲池（4 的倍数进 cs4，其余进 cs5），奇数归主组 cs1。
 * 只有归属牺牲池的人才参与收敛，所以奇数 uid 用来验证「其他组凌晨不受影响」。
 */
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
    $assignments = [];
    for ($uid = 0; $uid <= 200; $uid++) {
        $assignments[(string) $uid] = $uid % 2 === 1
            ? 'p_cs1'
            : ($uid % 4 === 0 ? 'p_cs4' : 'p_cs5');
    }
    return ['pools' => $pools, 'assignments' => $assignments];
}

/** 归属牺牲池的 uid，用来跑那些跟归属无关的分摊断言。 */
const MEMBER_UID = 12;

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
function targets(
    RouterTestService $s,
    ?array $override = null,
    int $uid = MEMBER_UID
): array {
    return $s->callValue('nightConvergeTargets', router(), $override, $uid);
}

function pick(RouterTestService $s, array $t, int $uid): string
{
    return $s->callValue('convergeTargetForUser', $t, $uid);
}

$hour = (int) date('G');
$inWindow = $hour >= 1 && $hour < 9;
$full = svc(['night_converge_start' => 0, 'night_converge_end' => 24]);

T::case('不该收敛的情形一律返回空');
T::same([], targets(svc(['night_converge_enabled' => false])), '开关关掉');
T::same([], targets(svc(['night_converge_pool_ids' => ''])), '牺牲池留空');
T::same([], targets(svc(['night_converge_pool_ids' => 'dead'])), '牺牲池已停用');
T::same([], targets($full, ['pool_id' => 'p_cs1']), '人工锁定的用户不参与');

// 排查靠的是面板上看得见的分组：一组整晚没被墙就整批挪走，再对剩下的对半分。
// 所以夜里的落点必须等于面板上的归属，否则挪了人夜里还是按老样子下发。
T::case('落点就是静态归属');
T::same(['p_cs4'], targets($full, null, 4), '归属 cs4 的人夜里落 cs4');
T::same(['p_cs5'], targets($full, null, 6), '归属 cs5 的人夜里落 cs5');
T::same(['p_cs4'], targets($full, null, MEMBER_UID), '同一个人始终落自己那组');
T::same([], targets($full, null, 7), '归属主组的人凌晨保持原 IP');
T::same([], targets($full, null, 999999), '归属表里没有的人不参与');
T::same(
    ['p_cs5'],
    targets(
        svc([
            'night_converge_pool_ids' => 'dead,cs5',
            'night_converge_start' => 0,
            'night_converge_end' => 24,
        ]),
        null,
        6
    ),
    '停用的池被过滤掉，可用的留下'
);
T::same(
    [],
    targets(
        svc([
            'night_converge_pool_ids' => 'dead,cs5',
            'night_converge_start' => 0,
            'night_converge_end' => 24,
        ]),
        null,
        4 // 归属 cs4，但 cs4 这轮没被选为牺牲池
    ),
    '归属的池没被选中就不收敛'
);

// 8/27 的回归用例。收全站时主组窗口内零流量，探测找不到有流量的地址，主组的 IP
// 才活得下来；只收自己人的那两晚，主组凌晨有了流量，五个组全被墙。
T::case('收全站时其余组的人也进牺牲池');
$openAll = svc([
    'night_converge_start' => 0,
    'night_converge_end' => 24,
    'night_converge_members_only' => false,
]);
T::same(['p_cs5'], targets($openAll, null, 7), '归属主组的人落最后一个牺牲池');
T::same(['p_cs5'], targets($openAll, null, 999999), '归属表里没有的人也一起收');
T::same(
    ['p_cs4'],
    targets($openAll, null, 4),
    '归属牺牲池的人仍落自己那个，第一个池的嫌疑人纯度不被冲掉'
);
T::same(['p_cs5'], targets($openAll, null, 6), '归属最后一个牺牲池的人不变');
T::same([], targets($openAll, ['pool_id' => 'p_cs1']), '人工锁定的照旧不参与');

// 这一组是 8/24 那次事故的回归用例：当时按节点分摊，每个人同时占住两个牺牲地址，
// 招墙的一个人就把两个一起带走，一夜三十六次墙里十五组成对、多组间隔仅 1 秒。
T::case('一个人只占一个牺牲地址');
$t = ['p_cs4', 'p_cs5'];
T::same(
    1,
    count(array_unique([
        pick($full, $t, 2367),
        pick($full, $t, 2367),
        pick($full, $t, 2367),
    ])),
    '同一个人反复取只会拿到同一个池，不会横跨两个地址'
);

T::case('落点是定死的');
$seen = [];
for ($i = 0; $i < 5; $i++) {
    $seen[] = pick($full, $t, 12345);
}
T::same(1, count(array_unique($seen)), '反复拉订阅不会来回换地址');

T::case('不同用户分摊到两个牺牲池');
$byUser = [];
for ($uid = 1; $uid <= 40; $uid++) {
    $byUser[pick($full, $t, $uid)][] = $uid;
}
T::same(2, count($byUser), '人被分成两半，哪个池死就说明人在哪半边');
T::ok(
    count($byUser['p_cs4']) >= 15 && count($byUser['p_cs5']) >= 15,
    '两边大致均分：cs4 拿 ' . count($byUser['p_cs4']) . ' 人，cs5 拿 ' . count($byUser['p_cs5']) . ' 人'
);

T::case('只配一个牺牲池时退化为单地址');
$one = ['p_cs5'];
$all = [];
for ($uid = 1; $uid <= 10; $uid++) {
    $all[] = pick($full, $one, $uid);
}
T::same(['p_cs5'], array_values(array_unique($all)), '全部落同一个池，不报错');

// 限定只收自己人时 targets 只剩他归属的那一个，取模必然落在它身上——
// 这条链路是「面板上挪了人，夜里就跟着变」的全部依据。
T::case('限定收自己人时落点等于面板上的组');
foreach ([4 => 'p_cs4', 8 => 'p_cs4', 6 => 'p_cs5', 10 => 'p_cs5'] as $uid => $expect) {
    T::same($expect, pick($full, targets($full, null, $uid), $uid), "uid {$uid} 落 {$expect}");
}

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
    $nowIn ? ['p_cs4'] : [],
    targets($live),
    '当前 ' . $hour . ' 点' . ($nowIn ? '应当收敛到自己那组' : '应当不收敛')
);
T::same([], targets($live, null, 7), '主组的人任何时刻都不被收敛');

// ── 自动隔离跟墙用户
//
// 判据只有一条：攒够墙次数后，每次都实际拿到过死地址的人被挪进第一个牺牲池。
// 上一版按单次曝光挪人被废掉了，因为一个地址被墙时拿到过它的常有上百人。这里
// 要守住的就是「攒不够不动手」和「在场率不到不动手」，松一格就是当年那个误伤。

/** 自动隔离要跑在窗口内，用例统一开全天窗口。 */
function isoSvc(array $extra = []): RouterTestService
{
    return svc(array_merge([
        'night_converge_start' => 0,
        'night_converge_end' => 24,
        'night_auto_isolate_enabled' => true,
        'night_auto_isolate_min_walls' => 3,
        'night_auto_isolate_min_rate' => 100,
        'night_auto_isolate_daily_cap' => 20,
    ], $extra));
}

const CAMPAIGN = ['id' => 'legacy', 'target_group_ids' => [1]];

/**
 * 连开 $walls 次墙，每次的在场名单由 $per 给出。
 *
 * @param array<int, int[]> $per 第几次墙 => 该次在场的 uid
 * @return array{0: array, 1: int[]} 改动后的 router、累计被挪走的 uid
 */
function runWalls(RouterTestService $s, array $per, string $poolId = 'p_cs4'): array
{
    Redis::reset();
    $router = router();
    $moved = [];
    foreach ($per as $uids) {
        $out = $s->isolate(CAMPAIGN, $router, [$poolId => $uids]);
        $moved = array_merge($moved, $out['moved']);
    }
    return [$router, $moved];
}

T::case('攒不够墙次数不动手');
$s = isoSvc();
// 归属主组的奇数 uid：只有真被挪走才会变成 p_cs4
[$router, $moved] = runWalls($s, [[7, 9], [7, 9]]);
T::same([], $moved, '两次墙还差一次，一个人都不挪');
T::same('p_cs1', $router['assignments']['7'], '归属没被改动');

T::case('攒够次数且每次都在场才挪');
$s = isoSvc();
[$router, $moved] = runWalls($s, [[7, 9, 11], [7, 9, 13], [7, 15, 17]]);
T::same([7], $moved, '三次墙里只有 uid 7 次次在场');
T::same('p_cs4', $router['assignments']['7'], '归属被改到第一个牺牲池');
T::same('p_cs1', $router['assignments']['9'], '2/3 次在场的人留在原组');
T::ok(Log::has('自动隔离'), '挪人留了日志，事后能核对是谁在哪一晚被挪的');

// 8/29 那晚的教训：04:50 挪走一批之后牺牲B 又被墙五次，累积到八次却再没出过人，
// 因为在场率按全程算，后半夜才活跃起来的人凑不出「从第一次墙就在场」。
T::case('判完一轮清零，后半夜的人还能被抓到');
$s = isoSvc();
[$router, $moved] = runWalls($s, [
    [7, 9], [7, 9], [7, 9],      // 第一轮：7 和 9 全中
    [11, 13], [11, 13], [11, 13], // 第二轮：换成 11 和 13 全中
]);
sort($moved);
T::same([7, 9, 11, 13], $moved, '两轮各出一批，六次墙一共挪走四个人');
T::same('p_cs4', $router['assignments']['11'], '第二轮的人归属也改了');

T::case('在场率阈值');
$s = isoSvc(['night_auto_isolate_min_rate' => 60]);
[, $moved] = runWalls($s, [[7, 9], [7, 9], [7, 11]]);
sort($moved);
T::same([7, 9], $moved, '放到 60% 后 2/3 在场的人也进来');
$s = isoSvc();
[, $moved] = runWalls($s, [[7, 9], [7, 9], [7, 11]]);
T::same([7], $moved, '同样的数据在 100% 下只挪全中的那个');

T::case('关掉开关和窗口外都不动手');
[, $moved] = runWalls(isoSvc(['night_auto_isolate_enabled' => false]), [[7], [7], [7]]);
T::same([], $moved, '开关关掉');
[, $moved] = runWalls(
    isoSvc(['night_converge_start' => 3, 'night_converge_end' => 4]),
    [[7], [7], [7]]
);
T::same($hour === 3 ? [7] : [], $moved, '窗口外不判定（当前 ' . $hour . ' 点）');
[, $moved] = runWalls(isoSvc(['night_converge_pool_ids' => '']), [[7], [7], [7]]);
T::same([], $moved, '没配牺牲池时无处可挪');

T::case('已经在牺牲池里的人不重复挪');
$s = isoSvc();
// uid 4 归属就是 p_cs4
[$router, $moved] = runWalls($s, [[4], [4], [4]]);
T::same([], $moved, '归属已经是目标池，跳过');
T::same('p_cs4', $router['assignments']['4'], '归属保持不变');

T::case('人工锁定的用户不被自动挪走');
Redis::reset();
$s = isoSvc();
$router = router();
$router['overrides']['7'] = ['pool_id' => 'p_cs1', 'locked' => true, 'expires_at' => 0];
for ($i = 0; $i < 3; $i++) {
    $out = $s->isolate(CAMPAIGN, $router, ['p_cs4' => [7]]);
}
T::same([], $out['moved'], '锁定的人交给人工处置，自动化不插手');
T::same('p_cs1', $router['assignments']['7'], '归属没变');

T::case('单晚上限');
$s = isoSvc(['night_auto_isolate_daily_cap' => 2]);
[$router, $moved] = runWalls($s, [[7, 9, 11], [7, 9, 11], [7, 9, 11]]);
T::same(2, count($moved), '三个全中的人只挪走两个，剩下的等明晚');
$s = isoSvc(['night_auto_isolate_daily_cap' => 0]);
[, $moved] = runWalls($s, [[7], [7], [7]]);
T::same([], $moved, '上限设 0 等于停用');

// 8/30 那晚 02:59 挪满上限之后，每次墙都在配额检查处掉头，连零都没清，累积一路
// 涨到 14 次，后半夜再没判过一轮。配额用光只该停止挑人，不该停止分轮。
T::case('配额用光后仍然分轮清零');
$s = isoSvc(['night_auto_isolate_daily_cap' => 2]);
Redis::reset();
$router = router();
$walls = [];
$moved = [];
foreach ([[7, 9, 11], [7, 9, 11], [7, 9, 11], [13], [13], [13]] as $uids) {
    $out = $s->isolate(CAMPAIGN, $router, ['p_cs4' => $uids]);
    $walls[] = $out['walls']['p_cs4'];
    $moved = array_merge($moved, $out['moved']);
}
T::same([1, 2, 3, 1, 2, 3], $walls, '判完就清零，配额用光的后半夜照样分轮');
T::same(2, count($moved), '配额只放行两个人');
T::same('p_cs1', $router['assignments']['13'], '配额用光时够格的人也挪不动');

T::case('无效用户不挪');
$s = isoSvc();
$s->eligible = [9 => true];
[, $moved] = runWalls($s, [[7, 9], [7, 9], [7, 9]]);
T::same([9], $moved, '已过期/封禁的人不在归属表里折腾');

T::case('多个池各自独立攒次数');
Redis::reset();
$s = isoSvc();
$router = router();
// cs5 连墙三次，cs1 只墙一次
$s->isolate(CAMPAIGN, $router, ['p_cs5' => [7], 'p_cs1' => [9]]);
$s->isolate(CAMPAIGN, $router, ['p_cs5' => [7]]);
$out = $s->isolate(CAMPAIGN, $router, ['p_cs5' => [7]]);
T::same([7], $out['moved'], '攒够的池出人');
T::same('p_cs1', $router['assignments']['9'], '只墙一次的池不出人');
T::same(3, $out['walls']['p_cs5'], '各池的墙次数分开记');

T::case('跨零点窗口算作同一晚');
$night = fn(int $start, int $end): string => isoSvc([
    'night_converge_start' => $start,
    'night_converge_end' => $end,
])->callValue('convergeNightKey');
T::same(date('Y-m-d'), $night(0, 10), '不跨零点就是当天');
T::same(
    $hour < 6 ? date('Y-m-d', time() - 86400) : date('Y-m-d'),
    $night(22, 6),
    '跨零点窗口在凌晨算前一天，计数不会在零点断成两半'
);

exit(T::summary());
