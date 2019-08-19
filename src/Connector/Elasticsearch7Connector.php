<?php
/**
 * This file is part of the daikon-cqrs/elasticsearch7-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Daikon\Elasticsearch7\Connector;

use Daikon\Dbal\Connector\ConnectorInterface;
use Daikon\Dbal\Connector\ConnectorTrait;
use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;

final class Elasticsearch7Connector implements ConnectorInterface
{
    use ConnectorTrait;

    private function connect(): Client
    {
        $connectionDsn = [
            'scheme' => $this->settings['scheme'],
            'host' => $this->settings['host'],
            'port' => $this->settings['port'],
            'user' => $this->settings['user'],
            'pass' => $this->settings['password']
        ];

        return ClientBuilder::create()
            ->setHosts([$connectionDsn])
            ->build();
    }
}
