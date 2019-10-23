<?php
/**
 * This file is part of the daikon-cqrs/elasticsearch7-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Daikon\Elasticsearch7\Storage;

use Assert\Assertion;
use Daikon\Dbal\Exception\DbalException;
use Daikon\Elasticsearch7\Connector\Elasticsearch7Connector;
use Daikon\Metadata\Metadata;
use Daikon\ReadModel\Projection\ProjectionMap;
use Daikon\ReadModel\Query\QueryInterface;
use Daikon\ReadModel\Storage\ScrollAdapterInterface;
use Daikon\ReadModel\Storage\SearchAdapterInterface;
use Daikon\ReadModel\Storage\StorageAdapterInterface;
use Daikon\ReadModel\Storage\StorageResultInterface;
use Elasticsearch\Common\Exceptions\Missing404Exception;

final class Elasticsearch7StorageAdapter implements
    StorageAdapterInterface,
    SearchAdapterInterface,
    ScrollAdapterInterface
{
    /** @var Elasticsearch7Connector */
    private $connector;

    /** @var array */
    private $settings;

    public function __construct(Elasticsearch7Connector $connector, array $settings = [])
    {
        $this->connector = $connector;
        $this->settings = $settings;
    }

    public function read(string $identifier): StorageResultInterface
    {
        try {
            $document = $this->connector->getConnection()->get(
                array_merge($this->settings['read'] ?? [], [
                    'index' => $this->getIndex(),
                    'id' => $identifier
                ])
            );
            $projectionClass = $document['_source']['@type'];
            $projection = [$document['_id'] => $projectionClass::fromNative($document['_source'])];
        } catch (Missing404Exception $error) {
            // just return an empty result
        }

        return new Elasticsearch7StorageResult(
            new ProjectionMap($projection ?? [])
        );
    }

    public function write(string $identifier, array $data): bool
    {
        $document = array_merge($this->settings['write'] ?? [], [
            'index' => $this->getIndex(),
            'id' => $identifier,
            'body' => $data
        ]);

        $this->connector->getConnection()->index($document);

        return true;
    }

    public function delete(string $identifier): bool
    {
        throw new DbalException('Not implemented');
    }

    public function search(QueryInterface $query, int $from = null, int $size = null): StorageResultInterface
    {
        $query = array_merge($this->settings['search'] ?? [], [
            'index' => $this->getIndex(),
            'from' => $from,
            'size' => $size,
            'body' => $query->toNative(),
            'rest_total_hits_as_int' => true
        ]);

        $results = $this->connector->getConnection()->search($query);

        return new Elasticsearch7StorageResult(
            $this->makeProjectionMap($results['hits']['hits']),
            Metadata::fromNative(['total' => $results['hits']['total']])
        );
    }

    public function scrollStart(QueryInterface $query, int $size = null, $cursor = null): StorageResultInterface
    {
        $query = array_merge($this->settings['search'] ?? [], [
            'index' => $this->getIndex(),
            'size' => $size,
            'body' => $query->toNative(),
            'scroll' => $this->settings['scroll_timeout'] ?? '1m',
            'sort' => ['_doc'],
            'rest_total_hits_as_int' => true
        ]);

        $results = $this->connector->getConnection()->search($query);

        return new Elasticsearch7StorageResult(
            $this->makeProjectionMap($results['hits']['hits']),
            Metadata::fromNative([
                'total' => $results['hits']['total'],
                'cursor' => $results['_scroll_id']
            ])
        );
    }

    public function scrollNext($cursor, int $size = null): StorageResultInterface
    {
        Assertion::string($cursor);
        Assertion::notEmpty($cursor);

        $results = $this->connector->getConnection()->scroll([
            'scroll_id' => $cursor,
            'scroll' => $this->settings['scroll_timeout'] ?? '1m',
            'rest_total_hits_as_int' => true
        ]);

        return new Elasticsearch7StorageResult(
            $this->makeProjectionMap($results['hits']['hits']),
            Metadata::fromNative([
                'total' => $results['hits']['total'],
                'cursor' => $results['_scroll_id']
            ])
        );
    }

    public function scrollEnd($cursor): void
    {
        Assertion::string($cursor);
        Assertion::notEmpty($cursor);

        $this->connector->getConnection()->clearScroll(['scroll_id' => $cursor]);
    }

    private function getIndex(): string
    {
        return $this->settings['index'] ?? $this->connector->getSettings()['index'];
    }

    private function makeProjectionMap(array $documents): ProjectionMap
    {
        $projections = [];
        foreach ($documents as $document) {
            $projectionClass = $document['_source']['@type'];
            $projections[$document['_id']] = $projectionClass::fromNative($document['_source']);
        }
        return new ProjectionMap($projections);
    }
}
