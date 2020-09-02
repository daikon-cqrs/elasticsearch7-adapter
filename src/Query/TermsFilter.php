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

final class TermsFilter implements QueryInterface
{
    private array $query;

    /** @param array $terms */
    public static function fromNative($terms): QueryInterface
    {
        Assert::that($terms)->isArray('Must be an array.')->notEmpty('Must not be empty.');
        return new self($terms);
    }

    public function toNative(): array
    {
        return $this->query;
    }

    private function __construct(array $terms = [])
    {
        $filterTerms = [];
        foreach ($terms as $term => $value) {
            if (empty($value)) {
                continue;
            }
            $filterTerms[] = ['term' => [$term => $value]];
        }

        $this->query = [
            'query' => [
                'bool' => [
                    'filter' => $filterTerms
                ]
            ]
        ];
    }
}
