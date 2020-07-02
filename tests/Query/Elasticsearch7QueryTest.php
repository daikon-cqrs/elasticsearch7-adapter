<?php declare(strict_types=1);
/**
 * This file is part of the daikon-cqrs/elasticsearch7-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Daikon\Tests\Elasticsearch7\Query;

use Daikon\Elasticsearch7\Query\Elasticsearch7Query;
use Daikon\Interop\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class Elasticsearch7QueryTest extends TestCase
{
    public function testFromNativeWithNullQuery(): void
    {
        $this->expectException(InvalidArgumentException::class);
        /** @psalm-suppress NullArgument */
        Elasticsearch7Query::fromNative(null);
    }

    public function testFromNativeWithStringQuery(): void
    {
        $this->expectException(InvalidArgumentException::class);
        /** @psalm-suppress InvalidArgument */
        Elasticsearch7Query::fromNative('query');
    }

    public function testToNative(): void
    {
        $query = Elasticsearch7Query::fromNative([]);
        $this->assertEquals([], $query->toNative());

        $payload = ['term' => 'value'];
        $query = Elasticsearch7Query::fromNative($payload);
        $this->assertEquals($payload, $query->toNative());
    }
}
