<?php declare(strict_types=1);
/**
 * This file is part of the daikon-cqrs/elasticsearch7-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Daikon\Elasticsearch7\Query;

use Daikon\Interop\Assert;
use Daikon\ReadModel\Query\QueryInterface;

final class TermFilter implements QueryInterface
{
    private array $query;

    /** @param array $term */
    public static function fromNative($term): QueryInterface
    {
        Assert::that($term)->isArray('Must be an array.')->notEmpty('Must not be empty.');
        return new self(key($term), current($term));
    }

    public function toNative(): array
    {
        return $this->query;
    }

    private function __construct(string $term, string $value)
    {
        $this->query = [
            'query' => [
                'bool' => [
                    'filter' => [
                        ['term' => [$term => $value]]
                    ]
                ]
            ]
        ];
    }
}
