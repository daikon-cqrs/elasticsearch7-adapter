<?php declare(strict_types=1);
/**
 * This file is part of the daikon-cqrs/elasticsearch7-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Daikon\Elasticsearch7\Storage;

use Daikon\Metadata\Metadata;
use Daikon\Metadata\MetadataInterface;
use Daikon\ReadModel\Projection\ProjectionInterface;
use Daikon\ReadModel\Projection\ProjectionMapInterface;
use Daikon\ReadModel\Storage\StorageResultInterface;

final class Elasticsearch7StorageResult implements StorageResultInterface
{
    private ProjectionMapInterface $projectionMap;

    private MetadataInterface $metadata;

    public function __construct(ProjectionMapInterface $projectionMap, MetadataInterface $metadata = null)
    {
        $this->projectionMap = $projectionMap;
        $this->metadata = $metadata ?? Metadata::makeEmpty();
    }

    public function getProjectionMap(): ProjectionMapInterface
    {
        return $this->projectionMap;
    }

    public function getMetadata(): MetadataInterface
    {
        return $this->metadata;
    }

    public function getFirst(): ?ProjectionInterface
    {
        if ($this->projectionMap->isEmpty()) {
            return null;
        }
        return $this->projectionMap->first();
    }

    public function getLast(): ?ProjectionInterface
    {
        if ($this->projectionMap->isEmpty()) {
            return null;
        }
        return $this->projectionMap->last();
    }

    public function isEmpty(): bool
    {
        return $this->projectionMap->isEmpty();
    }

    public function getIterator(): ProjectionMapInterface
    {
        return $this->projectionMap;
    }

    public function count(): int
    {
        return $this->projectionMap->count();
    }

    private function __clone()
    {
        $this->projectionMap = clone $this->projectionMap;
        $this->metadata = clone $this->metadata;
    }
}
