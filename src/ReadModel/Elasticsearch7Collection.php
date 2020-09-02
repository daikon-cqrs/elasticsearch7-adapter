<?php declare(strict_types=1);
/**
 * This file is part of the daikon-cqrs/elasticsearch7-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Daikon\Elasticsearch7\ReadModel;

use Daikon\Elasticsearch7\Query\Elasticsearch7Query;
use Daikon\Elasticsearch7\Query\IdsQuery;
use Daikon\ReadModel\Query\QueryInterface;
use Daikon\ReadModel\Repository\RepositoryInterface;
use Daikon\ReadModel\Repository\RepositoryMap;
use Daikon\ReadModel\Storage\StorageResultInterface;

abstract class Elasticsearch7Collection
{
    protected RepositoryInterface $repository;

    abstract public function __construct(RepositoryMap $repositoryMap);

    public function byId(string $id): StorageResultInterface
    {
        return $this->repository->findById($id);
    }

    public function byIds(array $ids, int $from = 0, int $size = 50): StorageResultInterface
    {
        $query = IdsQuery::fromNative($ids);
        return $this->repository->search($query, $from, $size);
    }

    /** @param array|QueryInterface $query */
    public function search($query, int $from = 0, int $size = 50): StorageResultInterface
    {
        if (is_array($query)) {
            $query = Elasticsearch7Query::fromNative($query);
        };

        return $this->repository->search($query, $from, $size);
    }

    /** @param array|QueryInterface $query */
    public function walk($query, callable $callback, int $size = 50): void
    {
        if (is_array($query)) {
            $query = Elasticsearch7Query::fromNative($query);
        };

        $this->repository->walk($query, $callback, $size);
    }

    /** @param array|QueryInterface $query */
    public function selectOne($query): StorageResultInterface
    {
        return $this->search($query, 0, 1);
    }
}
