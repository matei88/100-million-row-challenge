<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    private const int WORKER_COUNT = 8;

    private array $routeMap = [];
    private array $routeList = [];
    private array $dateChars = [];  // "5-01-24" => packed 2-byte id
    private array $dateList = [];   // id => "2025-01-24"

    public function __construct()
    {
        $this->buildRouteMap();
    }

    private function workerFile(int $i): string
    {
        return sys_get_temp_dir() . "/100m_worker_{$i}.bin";
    }

    private function buildRouteMap(): void
    {
        foreach (Visit::all() as $id => $visit) {
            $path = substr($visit->uri, 25);
            $this->routeMap[$path] = $id;
            $this->routeList[$id] = $path;
        }

        // Pre-pack date lookup: "5-01-24" (7 chars after year digit) => 2-byte packed id
        $epoch = strtotime('2021-01-01');
        $dateCount = 0;
        for ($d = 0; $d < 2200; $d++) {
            $full = date('Y-m-d', $epoch + $d * 86400);
            $this->dateChars[substr($full, 3)] = pack('v', $dateCount);
            $this->dateList[$dateCount] = $full;
            $dateCount++;
        }
    }

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);
        $chunkSize = (int) ceil($fileSize / self::WORKER_COUNT);

        // Align chunks to newlines
        $boundaries = [0];
        $handle = fopen($inputPath, 'rb');
        for ($i = 1; $i < self::WORKER_COUNT; $i++) {
            fseek($handle, $i * $chunkSize);
            fgets($handle);
            $boundaries[] = ftell($handle);
        }
        $boundaries[] = $fileSize;
        fclose($handle);

        $pids = [];

        for ($i = 0; $i < self::WORKER_COUNT; $i++) {
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

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Merge
        $pathCount = count($this->routeList);
        $dateCount = count($this->dateList);
        $counts = array_fill(0, $pathCount * $dateCount, 0);

        for ($i = 0; $i < self::WORKER_COUNT; $i++) {
            $path = $this->workerFile($i);
            $raw = file_get_contents($path);
            unlink($path);

            $childCounts = unpack('V*', $raw);
            $j = 0;
            foreach ($childCounts as $val) {
                $counts[$j++] += $val;
            }
        }

        // Build results
        $results = [];
        $base = 0;
        for ($p = 0; $p < $pathCount; $p++) {
            $route = $this->routeList[$p];
            for ($d = 0; $d < $dateCount; $d++) {
                $n = $counts[$base + $d];
                if ($n === 0) {
                    continue;
                }
                $results[$route][$this->dateList[$d]] = $n;
            }
            $base += $dateCount;
        }

        file_put_contents($outputPath, json_encode($results));
    }

    private function performTask(string $inputPath, int $start, int $end): array
    {
        $handle = fopen($inputPath, 'rb');
        fseek($handle, $start);

        $pathIds = &$this->routeMap;
        $dateChars = &$this->dateChars;
        $pathCount = count($this->routeList);

        // Each bucket is a string of packed 2-byte dateIds
        $buckets = array_fill(0, $pathCount, '');

        $bytesProcessed = 0;
        $toProcess = $end - $start;

        while ($bytesProcessed < $toProcess) {
            $remaining = $toProcess - $bytesProcessed;
            $chunk = fread($handle, min($remaining, 131072));
            if (!$chunk) {
                break;
            }

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                break;
            }

            // Rewind past incomplete trailing line
            $tail = strlen($chunk) - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
            }
            $bytesProcessed += $lastNl + 1;

            // Parse chunk: find comma, extract path and date
            $p = 0;

            // If first chunk starts mid-line (shouldn't with boundary alignment, but safety)
            if ($bytesProcessed === $lastNl + 1 && $start > 0) {
                $firstNl = strpos($chunk, "\n");
                if ($firstNl !== false) {
                    $p = $firstNl + 1;
                }
            }

            while ($p < $lastNl) {
                $c = strpos($chunk, ",", $p);
                if ($c === false || $c >= $lastNl) {
                    break;
                }

                $pathStr = substr($chunk, $p, $c - $p);
                $pathId = $pathIds[$pathStr] ?? null;

                if ($pathId !== null) {
                    // Date is at comma + 4 (skip "202"), take 7 chars ("5-01-24")
                    $dateKey = substr($chunk, $c + 4, 7);
                    if (isset($dateChars[$dateKey])) {
                        $buckets[$pathId] .= $dateChars[$dateKey];
                    }
                }

                // Skip to next line — find next newline
                $nl = strpos($chunk, "\n", $c);
                if ($nl === false) break;
                $p = $nl + 1;
            }
        }

        fclose($handle);
        return $buckets;
    }

    private function writeBuckets(int $workerId, array &$buckets): void
    {
        $pathCount = count($this->routeList);
        $dateCount = count($this->dateList);
        $counts = array_fill(0, $pathCount * $dateCount, 0);

        $base = 0;
        foreach ($buckets as $bucket) {
            if ($bucket !== '') {
                foreach (array_count_values(unpack('v*', $bucket)) as $dateId => $n) {
                    $counts[$base + $dateId] += $n;
                }
            }
            $base += $dateCount;
        }

        file_put_contents($this->workerFile($workerId), pack('V*', ...$counts));
    }
}