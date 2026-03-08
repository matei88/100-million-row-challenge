<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    private const int WORKER_COUNT = 8;

    private array $routeMap = [];
    private array $routeList = [];
    private array $dateChars = [];
    private array $dateList = [];
    private int $dateCount = 2200;
    private int $pathCount = 0;

    public function __construct()
    {
        $this->buildMaps();
    }

    private function workerFile(int $i): string
    {
        return sys_get_temp_dir() . "/100m_worker_{$i}.bin";
    }

    private function buildMaps(): void
    {
        foreach (Visit::all() as $id => $visit) {
            $slug = substr($visit->uri, 25); // slug for CSV matching
            $path = substr($visit->uri, 19); // /blog/... for output
            $this->routeMap[$slug] = $id;
            $this->routeList[$id] = $path;
        }
        $this->pathCount = count($this->routeList);


        for ($i = 0; $i < $this->dateCount; $i++) {
            $z = $i + 737791;

            $era = intdiv($z,146097);
            $doe = $z - $era*146097;

            $yoe = intdiv($doe - intdiv($doe,1460) + intdiv($doe,36524) - intdiv($doe,146096),365);
            $y = $yoe + $era*400;

            $doy = $doe - (365*$yoe + intdiv($yoe,4) - intdiv($yoe,100));
            $mp = intdiv(5*$doy+2,153);

            $d = $doy - intdiv(153*$mp+2,5) + 1;
            $m = $mp + ($mp < 10 ? 3 : -9);
            $y += ($m <= 2);

            // ---- fast YYYY-MM-DD formatting ----
            $ym = $m < 10 ? '0'.$m : $m;
            $yd = $d < 10 ? '0'.$d : $d;
            $full = $y . '-' . $ym . '-' . $yd;

            $this->dateList[$i] = $full;
            $this->dateChars[substr($full,3)] = pack('v',$i);
        }
    }

    /**
     * Quick scan of first ~200KB to determine URL encounter order
     */
    private function discoverOrder(string $inputPath): array
    {
        $handle = fopen($inputPath, 'rb');
        $chunk = fread($handle, 204800);
        fclose($handle);

        $seen = [];
        $lastNl = strrpos($chunk, "\n");
        $p = 0;

        while ($p < $lastNl) {
            $c = strpos($chunk, ",", $p);
            if ($c === false) {
                break;
            }

            $slug = substr($chunk, $p + 25, $c - $p - 25);
            if (!isset($seen[$slug]) && isset($this->routeMap[$slug])) {
                $seen[$slug] = $this->routeMap[$slug];
            }

            $nl = strpos($chunk, "\n", $c);
            if ($nl === false) {
                break;
            }
            $p = $nl + 1;
        }

        $order = array_values($seen);
        $inOrder = array_flip($order);

        for ($i = 0; $i < $this->pathCount; $i++) {
            if (!isset($inOrder[$i])) {
                $order[] = $i;
            }
        }

        return $order;
    }

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);
        $routeOrder = $this->discoverOrder($inputPath);

        // Align chunks to newlines
        $boundaries = [0];
        $handle = fopen($inputPath, 'rb');
        for ($i = 1; $i < self::WORKER_COUNT; $i++) {
            fseek($handle, (int)(($fileSize * $i) / self::WORKER_COUNT));
            fgets($handle);
            $boundaries[] = ftell($handle);
        }
        $boundaries[] = $fileSize;
        fclose($handle);

        $pids = [];

        for ($i = 0; $i < self::WORKER_COUNT - 1; $i++) {
            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new \RuntimeException('Fork failed');
            }

            if ($pid === 0) {
                $buckets = $this->performTask($inputPath, $boundaries[$i], $boundaries[$i + 1]);
                $this->writeBuckets($i, $buckets);
                exit(0);
            }

            $pids[] = $pid;
        }

        // Parent takes the last chunk
        $lastChunk = self::WORKER_COUNT - 1;
        $buckets = $this->performTask($inputPath, $boundaries[$lastChunk], $boundaries[$lastChunk + 1]);
        $this->writeBuckets($lastChunk, $buckets);

        // Wait for children
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Merge
        $counts = array_fill(0, $this->pathCount * $this->dateCount, 0);

        for ($i = 0; $i < self::WORKER_COUNT; $i++) {
            $path = $this->workerFile($i);
            $raw = file_get_contents($path);
            unlink($path);

            $len = strlen($raw);
            $j = 0;
            $chunkBytes = 20000; // 5000 ints
            for ($offset = 0; $offset < $len; $offset += $chunkBytes) {
                $slice = unpack('V*', substr($raw, $offset, $chunkBytes));
                foreach ($slice as $v) {
                    $counts[$j++] += $v;
                }
            }
        }

        // Build results: URLs in input order, dates sorted asc
        $results = [];
        foreach ($routeOrder as $p) {
            $route = $this->routeList[$p];
            $base = $p * $this->dateCount;

            // dateList is already chronological, so iterating 0..n is sorted asc
            for ($d = 0; $d < $this->dateCount; $d++) {
                $n = $counts[$base + $d];
                if ($n === 0) {
                    continue;
                }
                $results[$route][$this->dateList[$d]] = $n;
            }
        }

        file_put_contents($outputPath, json_encode($results, JSON_PRETTY_PRINT));
    }

    private function performTask(string $inputPath, int $start, int $end): array
    {
        $handle = fopen($inputPath, 'rb');
        fseek($handle, $start);

        $pathIds = &$this->routeMap;
        $dateChars = &$this->dateChars;

        $buckets = array_fill(0, $this->pathCount, '');

        $bytesProcessed = 0;
        $toProcess = $end - $start;

        while ($bytesProcessed < $toProcess) {
            $remaining = $toProcess - $bytesProcessed;
            $chunk = fread($handle, $remaining > 131072 ? 131072 : $remaining);
            if (!$chunk) {
                break;
            }

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                break;
            }

            $tail = strlen($chunk) - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
            }
            $bytesProcessed += $lastNl + 1;

            $p = 0;

            while ($p < $lastNl) {
                $c = strpos($chunk, ",", $p);
                if ($c === false || $c >= $lastNl) {
                    break;
                }

                // Extract slug: skip 25-char URL prefix
                $slug = substr($chunk, $p + 25, $c - $p - 25);
                $pathId = $pathIds[$slug] ?? null;

                if ($pathId !== null) {
                    $dateKey = substr($chunk, $c + 4, 7);
                    if ($dateChars[$dateKey]) {
                        $buckets[$pathId] .= $dateChars[$dateKey];
                    }
                }

                $nl = strpos($chunk, "\n", $c);
                if ($nl === false) {
                    break;
                }
                $p = $nl + 1;
            }
        }

        fclose($handle);
        return $buckets;
    }

    private function writeBuckets(int $workerId, array &$buckets): void
    {
        $total = $this->pathCount * $this->dateCount;
        $counts = array_fill(0, $total, 0);

        $base = 0;
        foreach ($buckets as $bucket) {
            if ($bucket !== '') {
                $len = strlen($bucket);
                for ($i = 0; $i < $len; $i += 2) {
                    $dateId = ord($bucket[$i]) | (ord($bucket[$i + 1]) << 8);
                    $counts[$base + $dateId]++;
                }
            }
            $base += $this->dateCount;
        }

        $binary = pack('V*', ...$counts);
        file_put_contents($this->workerFile($workerId), $binary);
    }
}