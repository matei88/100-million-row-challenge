<?php

namespace App\Commands;

use Tempest\Console\ConsoleCommand;
use Tempest\Console\HasConsole;
use Tempest\Console\Middleware\ForceMiddleware;
use function Tempest\Intl\Number\parse_int;

final class DataGenerateCommand
{
    use HasConsole;

    #[ConsoleCommand(middleware: [ForceMiddleware::class])]
    public function __invoke(
        int|string $iterations = 1_000_000,
        string $outputPath = __DIR__ . '/../../data/data.csv',
    ): void
    {
        $iterations = parse_int(str_replace([',', '_'], '', $iterations));

        if (! $this->confirm(sprintf(
            "Generating data for %s iterations in %s. Continue?",
            number_format($iterations),
            $outputPath,
        ), default: true)) {
            $this->error('Cancelled');
            return;
        }

        $visits = Visit::all();

        $handle = fopen($outputPath, 'w');

        $i = 0;

        while ($i < $iterations) {
            $visit = $visits[array_rand($visits)];

            fwrite($handle, $visit->uri . ';' . $visit->generateDate() . PHP_EOL);

            $i++;

            if ($i % 100_000 === 0) {
                $this->info('Generated ' . number_format($i) . ' rows');
            }
        }

        fclose($handle);

        $this->success('Done');
    }
}