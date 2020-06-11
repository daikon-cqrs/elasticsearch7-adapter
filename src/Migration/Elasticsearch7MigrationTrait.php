<?php declare(strict_types=1);
/**
 * This file is part of the daikon-cqrs/elasticsearch7-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Daikon\Elasticsearch7\Migration;

use Daikon\Dbal\Exception\DbalException;
use Daikon\Dbal\Migration\MigrationTrait;
use Elasticsearch\Common\Exceptions\Missing404Exception;

trait Elasticsearch7MigrationTrait
{
    use MigrationTrait;

    private function createIndex(string $index, array $settings = []): void
    {
        $indices = $this->connector->getConnection()->indices();

        if ($this->indexExists($index)) {
            throw new DbalException("Cannot create already existing index '$index'.");
        }

        $indices->create(['index' => $index, 'body' => $settings]);
    }

    private function createAlias(string $index, string $alias): void
    {
        $indices = $this->connector->getConnection()->indices();
        $indices->updateAliases([
            'body' => [
                'actions' => [['add' => ['index' => $index, 'alias' => $alias]]]
            ]
        ]);
    }

    private function reassignAlias(string $index, string $alias): void
    {
        $currentIndices = $this->getIndicesWithAlias($alias);
        if (count($currentIndices) !== 1) {
            throw new DbalException(
                "Cannot reassign alias '$alias' since it is not assigned to exactly one index."
            );
        }

        $indices = $this->connector->getConnection()->indices();
        $indices->updateAliases([
            'body' => [
                'actions' => [
                    ['remove' => ['index' => current($currentIndices), 'alias' => $alias]],
                    ['add' => ['index' => $index, 'alias' => $alias]]
                ]
            ]
        ]);
    }

    private function deleteIndex(string $index): void
    {
        $indices = $this->connector->getConnection()->indices();

        if (!$this->indexExists($index)) {
            throw new DbalException("Cannot delete non-existing index '$index'.");
        }

        $indices->delete(['index' => $index]);
    }

    private function putMapping(string $index, array $mapping): void
    {
        $indices = $this->connector->getConnection()->indices();
        $indices->putMapping(['index' => $index, 'body' => $mapping]);
    }

    private function reindexWithMapping(string $source, string $dest, array $mapping): void
    {
        $settings = $this->getIndexSettings($source);
        $mappings['mappings'] = $mapping;
        $this->createIndex($dest, array_merge($settings, $mappings));
        $this->reindex($source, $dest);
    }

    private function reindex(string $source, string $dest)
    {
        $client = $this->connector->getConnection();
        $client->reindex([
           'body' => [
               'source' => ['index' => $source],
               'dest' => ['index' => $dest, 'version_type' => 'external']
           ]
        ]);
    }

    private function getIndexSettings(string $index): array
    {
        $indices = $this->connector->getConnection()->indices();
        $settings = current($indices->getSettings(['index' => $index]));
        // have to remove info settings to create new index..
        unset($settings['settings']['index']['uuid']);
        unset($settings['settings']['index']['version']);
        unset($settings['settings']['index']['creation_date']);
        unset($settings['settings']['index']['provided_name']);
        return $settings;
    }

    private function getIndicesWithAlias(string $alias): array
    {
        $indices = $this->connector->getConnection()->indices();

        try {
            $indexNames = array_keys($indices->getAlias(['name' => $alias]));
        } catch (Missing404Exception $error) {
        }

        return $indexNames ?? [];
    }

    private function indexExists(string $index): bool
    {
        $indices = $this->connector->getConnection()->indices();
        return $indices->exists(['index' => $index]);
    }

    private function getIndexPrefix()
    {
        return $this->connector->getSettings()['index_prefix'];
    }
}
