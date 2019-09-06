<?php
/**
 * This file is part of the daikon-cqrs/elasticsearch7-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Daikon\Elasticsearch7\Storage;

use Daikon\Dbal\Exception\DbalException;
use Daikon\Elasticsearch7\Connector\Elasticsearch7Connector;
use Daikon\Metadata\Metadata;
use Daikon\ReadModel\Projection\ProjectionMap;
use Daikon\ReadModel\Query\QueryInterface;
use Daikon\ReadModel\Storage\SearchAdapterInterface;
use Daikon\ReadModel\Storage\StorageAdapterInterface;
use Daikon\ReadModel\Storage\StorageResultInterface;
use Elasticsearch\Common\Exceptions\Missing404Exception;

final class Elasticsearch7StorageAdapter implements StorageAdapterInterface, SearchAdapterInterface
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
            'body' => $query->toNative()
        ]);

        $results = $this->connector->getConnection()->search($query);

        $projections = [];
        foreach ($results['hits']['hits'] as $document) {
            $projectionClass = $document['_source']['@type'];
            $projections[$document['_id']] = $projectionClass::fromNative($document['_source']);
        }

        return new Elasticsearch7StorageResult(
            new ProjectionMap($projections),
            Metadata::fromNative(['total' => $results['hits']['total']['value']])
        );
    }

    private function getIndex(): string
    {
        return $this->settings['index'] ?? $this->connector->getSettings()['index'];
    }
}
