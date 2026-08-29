<?php

/**
 * 让路由逻辑脱离 Laravel 跑起来。
 *
 * 下发决策本身只是「状态数组 + 时间 → 池子顺序」的纯计算，唯一的外部依赖
 * 是写日志和查「谁拉过这个 IP」。这里给前者一个能回放的假门面，后者由
 * RouterTestService 覆盖，于是整套用例不需要数据库、Redis 和 vendor。
 */

declare(strict_types=1);

namespace Illuminate\Support\Facades {
    class Log
    {
        /** @var list<array{level:string,message:string,context:array}> */
        public static array $records = [];

        public static function __callStatic(string $level, array $args): void
        {
            self::$records[] = [
                'level' => $level,
                'message' => (string) ($args[0] ?? ''),
                'context' => (array) ($args[1] ?? []),
            ];
        }

        public static function reset(): void
        {
            self::$records = [];
        }

        /** 最近一条包含该关键字的日志，没有则返回 null。 */
        public static function find(string $needle): ?array
        {
            foreach (array_reverse(self::$records) as $r) {
                if (str_contains($r['message'], $needle)) {
                    return $r;
                }
            }
            return null;
        }

        public static function has(string $needle): bool
        {
            return self::find($needle) !== null;
        }
    }

    /**
     * 够用的内存版 Redis。
     *
     * 自动隔离的判定全靠计数攒够次数，返回 null 的空壳会让它永远不触发，用例也就
     * 永远是绿的。这里只实现用到的几个命令，语义跟真 Redis 一致：incr 从 0 起、
     * hgetall 返回字符串值。
     */
    class Redis
    {
        /** @var array<string, mixed> */
        public static array $store = [];

        public static function reset(): void
        {
            self::$store = [];
        }

        public static function get(string $key): ?string
        {
            return isset(self::$store[$key]) ? (string) self::$store[$key] : null;
        }

        public static function set(string $key, mixed $value): bool
        {
            self::$store[$key] = $value;
            return true;
        }

        public static function incr(string $key): int
        {
            return self::incrby($key, 1);
        }

        public static function incrby(string $key, int $by): int
        {
            self::$store[$key] = (int) (self::$store[$key] ?? 0) + $by;
            return self::$store[$key];
        }

        public static function hincrby(string $key, string $field, int $by): int
        {
            $hash = (array) (self::$store[$key] ?? []);
            $hash[$field] = (int) ($hash[$field] ?? 0) + $by;
            self::$store[$key] = $hash;
            return $hash[$field];
        }

        public static function hset(string $key, string $field, mixed $value): int
        {
            $hash = (array) (self::$store[$key] ?? []);
            $hash[$field] = $value;
            self::$store[$key] = $hash;
            return 1;
        }

        /** @return array<string, string> */
        public static function hgetall(string $key): array
        {
            $out = [];
            foreach ((array) (self::$store[$key] ?? []) as $field => $value) {
                $out[(string) $field] = (string) $value;
            }
            return $out;
        }

        public static function del(string ...$keys): int
        {
            $removed = 0;
            foreach ($keys as $key) {
                if (array_key_exists($key, self::$store)) {
                    unset(self::$store[$key]);
                    $removed++;
                }
            }
            return $removed;
        }

        public static function sadd(string $key, string $member): int
        {
            $set = (array) (self::$store[$key] ?? []);
            $set[$member] = true;
            self::$store[$key] = $set;
            return 1;
        }

        /** @return list<string> */
        public static function smembers(string $key): array
        {
            return array_map('strval', array_keys((array) (self::$store[$key] ?? [])));
        }

        public static function __callStatic(string $m, array $args): mixed
        {
            return null;
        }
    }

    class Cache
    {
        public static function __callStatic(string $m, array $args): mixed
        {
            return null;
        }
    }
}

namespace Plugin\BaitSplit\Tests {

    use Plugin\BaitSplit\Services\BaitSplitService;

    require_once __DIR__ . '/../Services/IpRotationClient.php';
    require_once __DIR__ . '/../Services/BaitSplitService.php';

    /**
     * 把「谁在什么时候拉过某个池的当前 IP」变成可编排的数据。
     * 线上这份数据来自 Redis 曝光表，测试里直接摆好。
     */
    class RouterTestService extends BaitSplitService
    {
        /** @var array<string, array<int,int>> poolId|ip => [uid => ts] */
        public array $exposure = [];

        /** 有效用户；null 表示不设限，任何 uid 都算有效。 */
        public ?array $eligible = null;

        protected function poolIpExposureLastMap(
            array $campaign,
            string $poolId,
            string $ip
        ): array {
            return $this->exposure[$poolId . '|' . $ip] ?? [];
        }

        /** 线上要查数据库，测试里直接摆好谁是有效用户。 */
        protected function eligibleUserIdSet(array $groupIds): array
        {
            if ($this->eligible !== null) {
                return $this->eligible;
            }
            $ids = [];
            for ($uid = 0; $uid <= 200; $uid++) {
                $ids[$uid] = true;
            }
            return $ids;
        }

        /**
         * 自动隔离按引用改归属，但引用是第二个参数，跟 call() 的约定不同。
         *
         * @param array<string, int[]> $suspectsByPool
         * @return array{moved: int[], walls: array<string, int>}
         */
        public function isolate(array $campaign, array &$router, array $suspectsByPool): array
        {
            $fn = \Closure::bind(
                function (array $c, array &$r, array $s): array {
                    return $this->autoIsolateFollowers($c, $r, $s);
                },
                $this,
                BaitSplitService::class
            );
            return $fn($campaign, $router, $suspectsByPool);
        }

        /** 记下某批人拉过某个池的当前 IP。 */
        public function pulled(array $router, string $poolId, array $uids, int $at): void
        {
            $ip = (string) ($router['pools'][$poolId]['host'] ?? '');
            $key = $poolId . '|' . $ip;
            foreach ($uids as $uid) {
                $this->exposure[$key][(int) $uid] = $at;
            }
        }

        /** 调用任意私有方法，第一个参数按引用传入 router。 */
        public function call(string $method, array &$router, ...$args): mixed
        {
            $fn = \Closure::bind(
                function (string $m, array &$r, array $rest) {
                    return $this->{$m}($r, ...$rest);
                },
                $this,
                BaitSplitService::class
            );
            return $fn($method, $router, $args);
        }

        /** 调用不需要引用传参的私有方法。 */
        public function callValue(string $method, ...$args): mixed
        {
            $fn = \Closure::bind(
                fn(string $m, array $rest) => $this->{$m}(...$rest),
                $this,
                BaitSplitService::class
            );
            return $fn($method, $args);
        }
    }

    /** 极简断言，够用且不引入依赖。 */
    final class T
    {
        public static int $passed = 0;
        /** @var list<string> */
        public static array $failures = [];
        private static string $case = '';

        public static function case(string $name): void
        {
            self::$case = $name;
            \Illuminate\Support\Facades\Log::reset();
            echo "\n▶ {$name}\n";
        }

        public static function ok(bool $cond, string $what): void
        {
            if ($cond) {
                self::$passed++;
                echo "  ✓ {$what}\n";
                return;
            }
            self::$failures[] = self::$case . ' → ' . $what;
            echo "  ✗ {$what}\n";
        }

        public static function same(mixed $expected, mixed $actual, string $what): void
        {
            $eq = $expected === $actual;
            self::ok($eq, $what . ($eq ? '' : sprintf(
                '（期望 %s，实际 %s）',
                var_export($expected, true),
                var_export($actual, true)
            )));
        }

        public static function summary(): int
        {
            $n = count(self::$failures);
            echo "\n" . str_repeat('─', 52) . "\n";
            if ($n === 0) {
                echo "全部通过：" . self::$passed . " 项断言\n";
                return 0;
            }
            echo "失败 {$n} 项（通过 " . self::$passed . "）：\n";
            foreach (self::$failures as $f) {
                echo "  - {$f}\n";
            }
            return 1;
        }
    }
}
