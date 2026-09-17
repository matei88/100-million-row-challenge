<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    private const int WORKER_COUNT = 8;
    private const int READ_SIZE = 8096;
    private const int WRITE_CHUNK_SIZE = 32648;

    private array $routeMap = [];
    private array $routeBaseMap = [];
    private array $routeList = [];
    private array $routeBase = [];
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
        $pathId = 0;

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, 25);
            $path = substr($visit->uri, 19);

            $this->routeMap[$slug] = $pathId;
            $this->routeList[$pathId] = $path;
            $this->routeBase[$pathId] = $pathId * $this->dateCount;
            $this->routeBaseMap[$slug] = $pathId * $this->dateCount;

            $pathId++;
        }

        $this->pathCount = $pathId;

        //$this->pathCount = count($this->routeList);

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
            //$this->dateChars[substr($full,3)] = pack('v',$i);
            $this->dateChars[substr($full, 3)] = $i;
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
                $counts = $this->performTask(
                    $inputPath,
                    $boundaries[$i],
                    $boundaries[$i + 1]
                );

                $this->writeCounts($i, $counts);
                exit(0);
            }

            $pids[] = $pid;
        }

        // Parent takes the last chunk
        $lastChunk = self::WORKER_COUNT - 1;

        $counts = $this->performTask(
            $inputPath,
            $boundaries[$lastChunk],
            $boundaries[$lastChunk + 1]
        );

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        for ($i = 0; $i < $lastChunk; $i++) {
            $path = $this->workerFile($i);

            $raw = file_get_contents($path);

            unlink($path);

            $len = strlen($raw);
            $j = 0;

            $chunkBytes = self::WRITE_CHUNK_SIZE;

            for (
                $offset = 0;
                $offset < $len;
                $offset += $chunkBytes
            ) {
                $slice = unpack(
                    'V*',
                    substr($raw, $offset, $chunkBytes)
                );

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

        //$routeMap = &$this->routeMap;
        $dateMap = &$this->dateChars;
        //$routeBase = &$this->routeBase;
        $routeBaseMap = &$this->routeBaseMap;

        $counts = array_fill(
            0,
            $this->pathCount * $this->dateCount,
            0,
        );

        $remaining = $end - $start;
        $leftover = '';

        while ($remaining > 0) {
            $readSize = min(
                self::READ_SIZE,
                $remaining
            );

            $raw = fread($handle, $readSize);
            if ($raw === '' || $raw === false) {
                break;
            }

            $len = strlen($raw);
            $remaining -= $len;

            $chunk = $leftover . $raw;
            $lastNl = strrpos($chunk, "\n");

            if ($lastNl === false) {
                $leftover = $chunk;
                continue;
            }

            $leftover = substr($chunk, $lastNl + 1);

            $p = 0;

            while ($p < $lastNl) {
                $comma = strpos($chunk, ',', $p + 25);

                ++$counts[
                    $routeBaseMap[
                        substr($chunk, $p + 25, $comma - $p - 25)
                    ]
                    + $dateMap[
                        substr($chunk, $comma + 4, 7)
                    ]
                ];

                $p = $comma + 27;
            }
        }

        fclose($handle);

        return $counts;
    }

    private function writeCounts(int $workerId, array $counts): void
    {
        $handle = fopen($this->workerFile($workerId), 'wb');

        $chunkSize = self::WRITE_CHUNK_SIZE;

        $total = count($counts);

        for ($i = 0; $i < $total; $i += $chunkSize) {
            $chunk = array_slice($counts, $i, $chunkSize);

            fwrite(
                $handle,
                pack('V*', ...$chunk)
            );
        }

        fclose($handle);
    }
}