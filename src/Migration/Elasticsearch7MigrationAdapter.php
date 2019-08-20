<?php
/**
 * This file is part of the daikon-cqrs/elasticsearch7-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Daikon\Elasticsearch7\Migration;

use Daikon\Dbal\Connector\ConnectorInterface;
use Daikon\Dbal\Exception\MigrationException;
use Daikon\Dbal\Migration\MigrationAdapterInterface;
use Daikon\Dbal\Migration\MigrationList;
use Daikon\Elasticsearch7\Connector\Elasticsearch7Connector;
use Elasticsearch\Common\Exceptions\Missing404Exception;

final class Elasticsearch7MigrationAdapter implements MigrationAdapterInterface
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

    public function read(string $identifier): MigrationList
    {
        $client = $this->connector->getConnection();

        try {
            $result = $client->get([
                'index' => $this->getIndex(),
                'id' => $identifier
            ]);
        } catch (Missing404Exception $error) {
            return new MigrationList;
        } catch (\Exception $error) {
            throw new MigrationException($error->getMessage(), $error->getCode(), $error);
        }

        return $this->createMigrationList($result['_source']['migrations']);
    }

    public function write(string $identifier, MigrationList $executedMigrations): void
    {
        $client = $this->connector->getConnection();
        $client->index([
            'index' => $this->getIndex(),
            'id' => $identifier,
            'body' => [
                'target' => $identifier,
                'migrations' => $executedMigrations->toNative()
            ]
        ]);
    }

    public function getConnector(): ConnectorInterface
    {
        return $this->connector;
    }

    private function createMigrationList(array $migrationData): MigrationList
    {
        $migrations = [];
        foreach ($migrationData as $migration) {
            $migrationClass = $migration['@type'];
            $migrations[] = new $migrationClass(new \DateTimeImmutable($migration['executedAt']));
        }
        return (new MigrationList($migrations))->sortByVersion();
    }

    private function getIndex(): string
    {
        return $this->settings['index'] ?? $this->connector->getSettings()['index'];
    }
}
