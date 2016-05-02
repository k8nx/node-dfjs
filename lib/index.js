import Immutable from 'immutable';

class Column {
    constructor(position, name, func = null, aggregation = false) {
        this.position = position;
        this.name = name;
        this.func = typeof func !== 'function' ? (element) => element : func;
        this.aggregation = aggregation;
    }

    /**
     *
     * @param name
     * @returns {Column}
     */
    rename(name) {
        return new Column(this.position, name, this.func, this.aggregation);
    }

    /**
     * builtin aggregation function
     *
     * @returns {Column}
     */
    sum() {
        const func = (values) => values.reduce((prev, current) => prev + current);
        return new Column(this.position, `sum(${this.name})`, func, true);
    }

    /**
     * builtin aggregation function
     *
     * @returns {Column}
     */
    max() {
        const func = (values) => values.reduce((prev, current) => prev > current ? prev : current);
        return new Column(this.position, `max(${this.name})`, func, true);
    }

    /**
     * builtin aggregation function
     *
     * @returns {Column}
     */
    min() {
        const func = (values) => values.reduce((prev, current) => prev < current ? prev : current);
        return new Column(this.position, `min(${this.name})`, func, true);
    }

    /**
     * builtin aggregation function
     *
     * @returns {Column}
     */
    avg() {
        const func = (values) => {
            const total = values.reduce((prev, current) => prev + current);
            const count = typeof values.count === 'function' ? values.count() : values.length;
            return total / count;
        };
        return new Column(this.position, `avg(${this.name})`, func, true);
    }

    /**
     * builtin aggregation function
     *
     * @returns {Column}
     */
    count() {
        const func = (values) => typeof values.count === 'function' ? values.count() : values.length;
        return new Column(this.position, `count(${this.name})`, func, true);
    }

    /**
     * custom function
     *
     * @param {String} name function name
     * @param {Function} func function
     * @param {Boolean} aggregation
     * @returns {Column}
     */
    transform(name, func, aggregation = false) {
        return new Column(this.position, `${name}(${this.name})`, func, aggregation);
    }

    /**
     * @param {Array<Array>} rows
     */
    reduce(rows) {
        return this.func(rows.map((element) => element[this.position]));
    }

    /**
     * @param {Array} row
     */
    map(row) {
        return this.func(row[this.position]);
    }
}

class DataFrame {
    constructor(data, keyPositions = null) {
        this._initialize(data, keyPositions);
    }

    /**
     *
     * @param {object} data
     * @param {Array<Number>} keyPositions
     * @private
     */
    _initialize(data, keyPositions = null) {
        const isDict = typeof data.get !== 'function';

        const meta = isDict ? data.meta : data.get('meta');
        const rows = isDict ? Immutable.List(data.rows) : data.get('rows');

        if (typeof data !== 'object' || typeof rows !== 'object') {
            throw new Error('invalid data');
        }

        const first = rows.get(0);
        if (rows.count() > 0 && first.length != 2 && first.length >= 2 && typeof first[1] !== 'object') {
            if (keyPositions == null) {
                throw new Error('invalid data. required `keyPositions`');
            }

            const excludePositions = Immutable.Map(keyPositions.map((element) => [element, true]));

            this.rows = rows.map((element) => [
                keyPositions.map((index) => element[index]),
                element.filter((_, index) => !excludePositions.get(index))
            ]);

            this.meta = Immutable.Map({
                keys: Immutable.List(this.rows.get(0)[0].map((_, index) => '_k' + index)),
                columns: Immutable.List(this.rows.get(0)[1].map((_, index) => '_c' + index))
            });
        } else {
            this.rows = rows;
            if (meta) {
                const keys = isDict ? meta.keys : meta.get('keys');
                const columns = isDict ? meta.columns : meta.get('columns');

                this.meta = Immutable.Map({
                    keys: keys && Immutable.List(keys),
                    columns: columns && Immutable.List(columns)
                });
            }
        }
        this._initializeMappings();
    }

    _initializeMappings() {
        this._mappings = Immutable.Map({
            keys: Immutable.Map(this.meta.get('keys').map((element, index) => [element, index])),
            columns: Immutable.Map(this.meta.get('columns').map((element, index) => [element, index]))
        });
    }

    keys(...keys) {
        return this._lookupPositions('keys', keys)
    }

    col(name) {
        const mappings = this._mappings.get('columns');
        const position = typeof name === 'number' && name < mappings.count() ? name : mappings.get(name);

        return new Column(position, this.meta.get('columns').get(position));
    }

    /**
     *
     * @param {Array<String>} names
     * @return {DataFrame}
     */
    select(...names) {
        const data = this._cloneMeta(null, this._lookupPositions('columns', names));
        const columns = names.map((name, index) => this.col(name));
        const rows = Immutable.List(this.rows.map(this._map(this._columns(columns))));

        return new DataFrame(data.set('rows', rows));
    }

    sort(desc = false) {
        return new DataFrame(this._cloneMeta()
            .set('rows', this.rows.sort((a, b) => !desc ? a[0] > b[0] : a[0] < b[0])));
    }

    collect(fn) {
        return this.rows.map((element) => {
            return fn(element[0], element[1]);
        });
    }

    /**
     *
     * @param {DataFrame} frame
     * @return {DataFrame}
     */
    merge(frame) {
        const data = this._mergeMeta(frame);
        const columnCounts = [
            this.meta.get('columns').count(),
            frame.meta.get('columns').count()
        ];
        const emptyColumns = columnCounts.map((element) => new Array(element));
        const keys2 = Immutable.Map(frame.rows.map((element, index) => [element[0].toString(), index]));

        const includes = this.rows.reduce((acc, curr) => {
            return acc.set(curr[0].toString(), typeof keys2.get(curr[0].toString()) === 'number');
        }, Immutable.Map());

        const rows = this.rows.map((element) => {
            const key = element[0];
            const has = typeof keys2.get(key.toString()) === 'number';
            return [key, element[1].concat(has ? frame.rows.get(keys2.get(key.toString()))[1] : emptyColumns[1])];
        });

        const additional = frame.rows.reduce((acc, element) => {
            const key = element[0];
            return includes.get(key.toString()) ? acc : acc.push([key, emptyColumns[0].concat(element[1])]);
        }, Immutable.List());

        return new DataFrame(data.set('rows', rows.concat(additional)));
    }

    /**
     * @param {Array} keys
     * @param {Array<Column>} columns
     * @return {DataFrame}
     */
    groupByKey(keys, ...columns) {
        const data = this._cloneMeta(keys);
        const meta = data.get('meta').set('columns', columns.map((element) => element.name));

        const groups = [];
        const rowIndexes = {};

        this.rows.forEach((element) => {
            const rowKey = keys == null ? 0 : this._lookupCells(element[0], keys);
            if (typeof rowIndexes[rowKey] !== 'number') {
                rowIndexes[rowKey] = groups.push([rowKey, []]) - 1;
            }
            groups[rowIndexes[rowKey]][1].push(element[1]);
        });

        const rows = Immutable.List(groups.map(this._reduce(this._columns(columns))));

        return new DataFrame(data.set('meta', meta).set('rows', rows));
    }

    _reduce(columns = null) {
        return (element) => {
            return [element[0], columns.map((col) => col.reduce(element[1]))];
        };
    }

    _map(columns = null) {
        return (element) => {
            return [element[0], columns.map((col) => col.map(element[1]))];
        };
    }

    /**
     * @param columns
     * @returns {Array<Column>}
     * @private
     */
    _columns(columns) {
        return columns.filter((column) => column && column.name);
    }

    _cloneMeta(keys = null, columns = null) {
        const meta = Immutable.Map({
            keys: this._lookupCells(this.meta.get('keys'), keys),
            columns: this._lookupCells(this.meta.get('columns'), columns)
        });

        return Immutable.Map({
            meta
        });
    }

    /**
     *
     * @param {DataFrame} frame
     * @return {DataFrame}
     * @private
     */
    _mergeMeta(frame) {
        const meta = Immutable.Map({
            keys: this._lookupCells(this.meta.get('keys'), null),
            columns: this.meta.get('columns').map((element) => `$1.${element}`)
                .concat(frame.meta.get('columns').map((element) => `$2.${element}`))
        });

        return Immutable.Map({
            meta
        });
    }

    _picks(collection, ids) {
        return collection.filter((element, index) => {
            return ids.indexOf(index);
        });
    }

    /**
     *
     * @param {Array<String>} cells
     * @param {Array<Number>} indexes
     * @returns {Array<String>}
     * @private
     */
    _lookupCells(cells, indexes) {
        if (indexes == null) {
            return cells;
        }

        return indexes.map((element) => typeof cells.get === 'function' ? cells.get(element) : cells[element]);
    }

    /**
     *
     * @param {String} mapping
     * @param {Array<String>|Array<Number>} names
     * @returns {Array<Number>}
     * @private
     */
    _lookupPositions(mapping, names) {
        const mappings = this._mappings.get(mapping);

        return names.map((name) => {
            if (typeof name === 'number' && name < mappings.count()) {
                return name;
            }

            return mappings.get(name);
        });
    }
}

module.exports = DataFrame;