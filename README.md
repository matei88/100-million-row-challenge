Your goal is to parse a data set of page visits into a JSON file.

## Setup

```sh
composer install
php tempest data:generate
```

Next, implement your solution in `app/Parser.php`. You can always run the parse command to check your work:

```sh
php tempest data:parse
```

Finally, before uploading your solution, make sure it validates:

```sh
php tempest data:validate
```

## Output formatting rules

The output file should contain the following:

- Each entry should be a key-value pair with the page's URL path as the key and the number of visits per day as the value
- Visits should be sorted by date in ascending order
- The output should be encoded as a pretty JSON string

### Example

Given the following input:

```csv
https://stitcher.io/blog/11-million-rows-in-seconds;2026-01-24T01:16:58+00:00
https://stitcher.io/blog/php-enums;2024-01-24T01:16:58+00:00
https://stitcher.io/blog/11-million-rows-in-seconds;2026-01-24T01:12:11+00:00
https://stitcher.io/blog/11-million-rows-in-seconds;2025-01-24T01:15:20+00:00
```

Should result in:

```json
{
    "\/blog\/11-million-rows-in-seconds": {
        "2025-01-24": 1,
        "2026-01-24": 2
    },
    "\/blog\/php-enums": {
        "2024-01-24": 1
    }
}
```

## Submitting your solution

Send a pull request to this repository with your solution. The title of your pull request should simply be your GitHub's username.