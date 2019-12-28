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
use Iterator;
use Traversable;

final class Elasticsearch7StorageResult implements StorageResultInterface
{
    /** @var ProjectionMapInterface */
    private $projectionMap;

    /** @var MetadataInterface */
    private $metadata;

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
        /** @var Iterator $iterator */
        $iterator = $this->getIterator();
        $iterator->rewind();
        return $iterator->current();
    }

    public function isEmpty(): bool
    {
        return $this->count() === 0;
    }

    public function getIterator(): Traversable
    {
        return $this->projectionMap->getIterator();
    }

    public function count(): int
    {
        return $this->projectionMap->count();
    }
}
