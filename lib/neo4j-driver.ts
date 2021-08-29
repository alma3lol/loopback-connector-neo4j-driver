import { Connector } from 'loopback-connector';
import { auth, Driver, driver, Session, Record, int  } from 'neo4j-driver';
import { ICallback, IDataSource } from './interfaces';
import _ from 'lodash';
import { v4 } from 'uuid';
import { Filter, Where } from '@loopback/repository';
import moment from 'moment';
import Debug from 'debug';
const debug = Debug('loopback:connector:neo4j-driver');

export const initialize = (datasource: IDataSource, callback: ICallback) => {
	const settings = datasource.settings;
	datasource.connector = new Neo4jConnector(settings.url, settings.username, settings.password, settings.database);
	return datasource.connector.connect(callback);
}

export class Neo4jConnector extends Connector {
	public driver: Driver;
	public session: Session;

	constructor(url: string, username: string, password: string, database: string = '') {
		super();
		this.driver = driver(url, auth.basic(username, password));
		this.session = this.driver.session({ database });
	}

	connect = (cb: ICallback) => {
		if (this.session) {
			return process.nextTick(() => cb(null, this.session))
		}
		this.session = this.driver.session()
		return process.nextTick(() => cb(null, this.session))
	}

	disconnect = (cb: ICallback) => {
		this.session.close();
		this.driver.close();
		process.nextTick(cb);
	}

	ping = (cb: ICallback) => {
		this.session.run('RETURN 1')
			.then(() => cb(null, true))
			.catch(cb);
	}

	create = (model: string, data: any, callback: ICallback) => {
		if (_.isUndefined(data.id)) data.id = v4();
		this.session.run(`CREATE (n:${_.upperFirst(model)} ${this.paramBlock(data)}) RETURN n`, this.datesToUnix(data))
			.then(() => callback(null, data.id))
			.catch(callback);
	}

	all = (model: string, filter: Filter, __: any, callback: ICallback) => {
		const { where, afterReturn, returnCypher } = Neo4jConnector.parseFilter(filter);
		this.session.run(`MATCH (n:${_.upperFirst(model)}) ${where[0]} RETURN ${returnCypher === '' ? 'n' : returnCypher } ${_.join(afterReturn.map(arr => arr[0]), ' ')}`, { ...where[1], ..._.assign({}, ...afterReturn.map(arr => arr[1])) })
			.then(res => callback(null, res.records.map(this.extractNode)))
			.catch(callback);
	}

	count = (model: string, where: Where, __: any, callback: ICallback) => {
		const [cypher, params] = Neo4jConnector.parseWhere(where);
		this.session.run(`MATCH (n:${_.upperFirst(model)}) ${cypher} RETURN count(n) as count`, params)
			.then(res => callback(null, res.records[0].toObject().count.low))
			.catch(callback);
	}

	destroyAll = (model: string, where: Where, __: any, callback: ICallback) => {
		const [cypher, params] = Neo4jConnector.parseWhere(where);
		this.session.run(`MATCH (n:${_.upperFirst(model)}) ${cypher} DELETE n RETURN n`, params)
			.then(result => callback(null, result.summary.counters.updates().nodesDeleted))
			.catch(callback);
	}

	update = (model: string, where: Where, data: any, __: any, callback: ICallback) => {
		const [cypher, params] = Neo4jConnector.parseWhere(where);
		this.session.run(`MATCH (n:${_.upperFirst(model)}) ${cypher} SET ${this.updateParams(data)} RETURN n`, { ...params, ...this.updateData(data) })
			.then(res => callback(null, { count: res.records.length }))
			.catch(callback);
	}

	destroy = (model: string, id: any,  __: any, callback: ICallback) => {
		this.session.run(`MATCH (n:${_.upperFirst(model)} {id: $id}) DELETE n RETURN n`, { id })
			.then(res => callback(null, res.summary.counters.updates().nodesDeleted))
			.catch(callback);
	}

	updateAttributes = (model: string, id: any, data: any,  __: any, callback: ICallback) => {
		this.session.run(`MATCH (n:${_.upperFirst(model)} {id: $id}) SET ${this.updateParams(data)} RETURN n`, { id, ...this.updateData(data) })
			.then(() => callback(null))
			.catch(callback);
	}

	replaceById = (model: string, id: any, data: any,  __: any, callback: ICallback) => {
		this.session.run(`MATCH (n:${_.upperFirst(model)} {id: $id}) SET ${this.updateParams(data)} RETURN n`, { id, ...this.updateData(data) })
			.then(() => callback(null))
			.catch(callback);
	}

	static parseFilter = (filter: Filter = {}) => {
		const parsedFilter: { where: [string, any], afterReturn: [string, any][], returnCypher: string } = { where: Neo4jConnector.parseWhere(filter.where), afterReturn: [], returnCypher: '' };
		if (filter.order && filter.order.length > 0) {
			parsedFilter.afterReturn.push([`ORDER BY ${_.join(filter.order.map(order => `n.${order}`), ', ')}`, {}])
		}
		if (filter.skip) {
			parsedFilter.afterReturn.push(['SKIP $skip', { skip: int(filter.skip) }])
		}
		if (filter.limit) {
			parsedFilter.afterReturn.push(['LIMIT $limit', { limit: int(filter.limit) }])
		}
		if (filter.fields) {
			const fieldsToReturn: string[] = [];
			for (const field in filter.fields) {
				if ((filter.fields as Object).hasOwnProperty(field)) fieldsToReturn.push(`n.${field}`);
			}
			parsedFilter.returnCypher = fieldsToReturn.length > 0 ? _.join(fieldsToReturn, ', ') : '';
		}
		return parsedFilter;
	}

	static parseWhere = (where: any = {}, _propCount: { [prop: string]: number } = {}) => {
		const parsedWhere: [string, any] = ['', {}];
		const andClause: [string, any] = ['', {}];
		const orClause: [string, any] = ['', {}];
		const cypherParts: [string, string, any][] = [];
		const propCount: { [prop: string]: number } = _propCount;
		if (_.isEqual(where, {})) return parsedWhere;
		if (_.isEqual(_propCount, {})) parsedWhere[0] += 'WHERE ';
		for(const prop in where) {
			if (_.includes(['and', 'or'], prop)) {
				let firstInClause = true;
				const parseAndOrClause = (clause: any, clauseResult: [string, any]) => {
					const result = Neo4jConnector.parseWhere(clause, propCount);
					if (clauseResult[0] === '') {
						clauseResult[0] = `${firstInClause ? '(' : ''}${result[0]}`;
					} else {
						clauseResult[0] += ` ${(prop === 'and') ? 'AND' : (!firstInClause && where[prop].length > 1) ? 'XOR' : 'OR'} ${firstInClause ? '(' : ''}${result[0]}`
					}
					clauseResult[1] = { ...clauseResult[1], ...result[1] };
					firstInClause = false;
				}
				where[prop].forEach((clause: any) => {
					parseAndOrClause(clause, prop === 'and' ? andClause : orClause);
				});
				if (prop === 'and') andClause[0] += ')';
				else orClause[0] += ')';
			} else {
				if (propCount[prop] === undefined) propCount[prop] = 0;
				if (_.includes([null, undefined], where[prop])) {
					cypherParts.push([`n.${prop} IS NULL`, '', where[prop]]);
				} else {
					if (typeof where[prop] === 'object') {
						const operator = Object.keys(where[prop])[0];
						const expression = where[prop][operator];
						if (_.includes(['inq', 'nin'], operator)) {
							if (Array.isArray(expression)) {
								propCount[prop]++;
								const propCountName = `${prop}${propCount[prop]}`;
								cypherParts.push([`${operator === 'nin' ? 'NOT ' : '' }n.${prop} IN \$${propCountName}`, propCountName, expression.map(val => (typeof val === 'number') ? int(val) : val)]);
							} else {
								debug('Unexpected value for inq/nin expression: %s. Array was expected', expression);
							}
						} else if (_.includes(['gt', 'gte', 'lt', 'lte'], operator)) {
							propCount[prop]++;
							const propCountName = `${prop}${propCount[prop]}`;
							cypherParts.push([`n.${prop} ${operator === 'gt' ? '>' : operator === 'gte' ? '>=' : operator === 'lt' ? '<' : '<='} \$${propCountName}`, propCountName, int(expression)]);
						} else if (_.includes(['like', 'nlike'], operator)) {
							propCount[prop]++;
							const propCountName = `${prop}${propCount[prop]}`;
							cypherParts.push([`${operator === 'nlike' ? 'NOT ' : '' }n.${prop} CONTAINS \$${propCountName}`, propCountName, expression]);
						} else if (operator === 'neq') {
							propCount[prop]++;
							const propCountName = `${prop}${propCount[prop]}`;
							cypherParts.push([`n.${prop} <> \$${propCountName}`, propCountName, typeof expression === 'number' ? int(expression) : expression]);
						} else {
							debug('Unsupported operator %s in where filter', operator);
						}
					} else {
						propCount[prop]++;
						const propCountName = `${prop}${propCount[prop]}`;
						cypherParts.push([`n.${prop} = \$${propCountName}`, propCountName, typeof where[prop] === 'number' ? int(where[prop]) : where[prop]]);
					}
				}
			}
		}
		const result: [string, any] = [_.join(cypherParts.map(part => part[0]), ' AND '), { ..._.assign({}, ...cypherParts.filter(part => part[1] !== '').map(part => ({ [part[1]]: part[2] }))) }]
		parsedWhere[0] += `${result[0]}`
		parsedWhere[1] = { ...parsedWhere[1], ...result[1] };
		parsedWhere[0] += andClause[0] !== '' ? ` AND ${andClause[0]}` : '';
		parsedWhere[1] = { ...parsedWhere[1], ...andClause[1] };
		parsedWhere[0] += orClause[0] !== '' ? ` OR ${orClause[0]}` : '';
		parsedWhere[1] = { ...parsedWhere[1], ...orClause[1] };
		return parsedWhere;
	}

	private paramBlock = (params: any) => {
		const paramNames = Object.keys(params)
		if (!paramNames.length) return ''
		const paramList = _.join(paramNames.map(p => `${p}: \$${p}`), ' ,')
		return `{${paramList}}`
	}

	private updateParams = (params: any) => {
		const paramNames = Object.keys(params)
		if (!paramNames.length) return ''
		const paramList = _.join(paramNames.map(p => `n.${p} = \$${p}Update`), ' ,')
		return `${paramList}`
	}

	private updateData = (data: any) => {
		const dataKeys = Object.keys(this.datesToUnix(data))
		if (!dataKeys.length) return {}
		const paramList: { [key: string]: any } = {};
		dataKeys.forEach(key => paramList[`${key}Update`] = data[key]);
		return paramList;
	}

	private extractNode = (record: Record) => record.toObject().n.properties

	private datesToUnix = (props: any = {}) => {
		return Object.entries(props).reduce(
			(data: any, [prop, val]) => {
				data[prop] = (val instanceof Date) ? moment(val).utc().valueOf() : val
				return data
			},
			{}
		)
	}
}
