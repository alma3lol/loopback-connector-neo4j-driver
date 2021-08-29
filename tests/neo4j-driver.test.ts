import { Filter, Where } from "@loopback/repository";
import { Neo4jConnector } from '../lib'
import _ from 'lodash';
import { int } from "neo4j-driver";

describe('Neo4j-driver', () => {
  it('should parse filter\'s limit, skip & order ', () => {
    const filter: Filter<{ id: number, name: string }> = {
      limit: 1,
      skip: 1,
      order: ['name ASC', 'id DESC']
    }
    const { afterReturn } = Neo4jConnector.parseFilter(filter);
    expect(_.join(afterReturn.map(arr => arr[0]), ' ')).toEqual('ORDER BY n.name ASC, n.id DESC SKIP $skip LIMIT $limit');
  })

  it('should parse filter\'s where', () => {
    const where: Where<{ id: number, name: string, age: number }> = {
      name: 'Test',
      age: null,
      or: [
        {
          name: { inq: ['Test'] },
        },
        {
          id: { gte: 1 }
        }
      ],
      and: [
        {
          name: { like: 'T' }
        },
        {
          age: { lte: 21 }
        }
      ]
    }
    const [ cypher, params ] = Neo4jConnector.parseWhere(where);
    expect(cypher).toEqual('WHERE n.name = $name1 AND n.age IS NULL AND (n.name CONTAINS $name3 AND n.age <= $age1) OR (n.name IN $name2 XOR n.id >= $id1)');
    expect(params).toEqual({ name1: 'Test', name2: ['Test'], id1: int(1), name3: 'T', age1: int(21) })
  });
  it('should parse filter\'s fields', () => {
    const filter: Filter<{ id: number, name: string, age: number }> = {
      fields: {
        age: true,
        name: true,
      },
    }
    const { returnCypher } = Neo4jConnector.parseFilter(filter);
    expect(returnCypher).toEqual('n.age, n.name');
  });
});
