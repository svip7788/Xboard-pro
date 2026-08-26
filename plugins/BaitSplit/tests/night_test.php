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

T::case('窗口内返回全部可用牺牲池');
T::same(['p_cs4', 'p_cs5'], targets($full), '两个池都要给出来，供按 uid 分摊');
T::same(
    ['p_cs5'],
    targets(
        svc([
            'night_converge_pool_ids' => 'dead,cs5',
            'night_converge_start' => 0,
            'night_converge_end' => 24,
        ]),
        null,
        6 // 归属 cs5，剩下的唯一可用牺牲池就是他自己那个
    ),
    '停用的池被过滤掉，可用的留下'
);

// 放开收全站时，凌晨每个拉订阅的人都被塞进这两个地址，主组、安静组的人跟着进来，
// 牺牲池被墙也读不出是谁招的——面板上牺牲组明明只有一千五百人，实际却在扛两千人。
T::case('只收敛归属牺牲池的人');
T::same(['p_cs4', 'p_cs5'], targets($full, null, 4), '归属 cs4 的人参与收敛');
T::same(['p_cs4', 'p_cs5'], targets($full, null, 6), '归属 cs5 的人参与收敛');
T::same([], targets($full, null, 7), '归属主组的人凌晨保持原 IP');
T::same([], targets($full, null, 999999), '归属表里没有的人不参与');
$openAll = svc([
    'night_converge_start' => 0,
    'night_converge_end' => 24,
    'night_converge_members_only' => false,
]);
T::same(
    ['p_cs4', 'p_cs5'],
    targets($openAll, null, 7),
    '关掉开关后退回全站收敛，主组的人也被收进来'
);

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

// uid 取模是均分，两边人群同质，8/25 和 8/26 两晚都是两个池各自被打穿，
// 死哪个都读不出信息。配上高危名单后「只死第一个池」才是可读的结果。
T::case('配了高危名单就按名单分，不再按 uid 均分');
$risk = svc([
    'night_converge_start' => 0,
    'night_converge_end' => 24,
    'night_risk_uids' => '7, 12,12 ,99,0,-3,abc',
]);
T::same('p_cs4', pick($risk, $t, 7), '名单里的人进第一个牺牲池');
T::same('p_cs4', pick($risk, $t, 12), '重复项不影响');
T::same('p_cs4', pick($risk, $t, 99), '名单里的人进第一个牺牲池');
T::same('p_cs5', pick($risk, $t, 8), '名单外的人进第二个');
T::same('p_cs5', pick($risk, $t, 100), '名单外的人进第二个');
T::same('p_cs5', pick($risk, $t, 0), '0 不是合法 uid，不算在名单里');
T::same(
    1,
    count(array_unique([pick($risk, $t, 7), pick($risk, $t, 7), pick($risk, $t, 7)])),
    '名单里的人反复拉订阅落点不变'
);

T::case('名单人群完整落在第一个池，其余完整落在第二个');
$byPool = [];
for ($uid = 1; $uid <= 120; $uid++) {
    $byPool[pick($risk, $t, $uid)][] = $uid;
}
T::same([7, 12, 99], $byPool['p_cs4'], '第一个池只装名单里的三个人');
T::same(117, count($byPool['p_cs5']), '其余 117 人全在第二个池');

T::case('名单为空或只有一个牺牲池时退回均分');
$blank = svc([
    'night_converge_start' => 0,
    'night_converge_end' => 24,
    'night_risk_uids' => ' , ,0',
]);
$byUser = [];
for ($uid = 1; $uid <= 40; $uid++) {
    $byUser[pick($blank, $t, $uid)][] = $uid;
}
T::same(2, count($byUser), '名单里没有合法 uid 时按 uid 均分');
T::same(
    ['p_cs5'],
    array_values(array_unique([
        pick($risk, $one, 7),
        pick($risk, $one, 8),
    ])),
    '只有一个牺牲池时名单不生效，都落这个池'
);

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
T::same([], targets($live, null, 7), '主组的人任何时刻都不被收敛');

exit(T::summary());
