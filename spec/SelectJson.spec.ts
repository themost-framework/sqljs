import '@testing-library/jest-dom/jest-globals';
import { expect } from '@jest/globals';
import {MemberExpression, MethodCallExpression, QueryEntity, QueryExpression, QueryField} from '@themost/query';
import { LocalSqlAdapter } from '@themost/sql.js';
import {SimpleOrder as SimpleOrderSchema} from './SimpleOrder';
import { fetch } from 'cross-fetch';

async function createSimpleOrders(db: LocalSqlAdapter) {
    const { source } = SimpleOrderSchema;
    const exists = await db.table(source).existsAsync();
    if (!exists) {
        await db.table(source).createAsync(SimpleOrderSchema.fields);    
    } else {
        return;
    }
    // get some orders
    const orders = await db.executeAsync<{ orderDate: Date, paymentMethod: number, 
        orderedItem: number, orderStatus: number, discount: number, customer: number
        discountCode: string, orderNumber: string, paymentDue: Date,
        dateCreated: Date, dateModified: Date, createdBy: number, modifiedBy: number }>(
        new QueryExpression().from('OrderBase').select(
            ({orderDate, discount, discountCode, orderNumber, paymentDue,
                 dateCreated, dateModified, createdBy, modifiedBy,
                 orderStatus, orderedItem, paymentMethod, customer}) => {
                return { orderDate, discount, discountCode, orderNumber, paymentDue,
                    dateCreated, dateModified, createdBy, modifiedBy,
                    orderStatus, orderedItem, paymentMethod, customer};
            })
            .orderByDescending((x: { orderDate: Date }) => x.orderDate).take(10), []
    );
    const paymentMethods = await db.executeAsync(
        new QueryExpression().from('PaymentMethodBase').select(
            ({id, name, alternateName, description}) => {
                return { id, name, alternateName, description };
            }), []
    );
    const orderStatusTypes = await db.executeAsync(
        new QueryExpression().from('OrderStatusTypeBase').select(
            ({id, name, alternateName, description}) => {
                return { id, name, alternateName, description };
        }), []
    );
    const orderedItems = await db.executeAsync(
        new QueryExpression().from('ProductData').select(
            ({id, name, category, model, releaseDate, price}) => {
                return { id, name, category, model, releaseDate, price };
            }), []
    );
    const customers = await db.executeAsync(
        new QueryExpression().from('PersonData').select(
            ({id, familyName, givenName, jobTitle, email, description, address}) => {
                return { id, familyName, givenName, jobTitle, email, description, address };
            }), []
    );
    const postalAddresses = await db.executeAsync(
        new QueryExpression().from('PostalAddressData').select(
            ({id, streetAddress, postalCode, addressLocality, addressCountry, telephone}) => {
                return {id, streetAddress, postalCode, addressLocality, addressCountry, telephone };
            }), []
    );

    const shuffleArray = (array: unknown[]) => {
        for (let i = array.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [array[i], array[j]] = [array[j], array[i]];
        }
        return array;
    };
    
    const getRandomItems = (array: unknown[], numItems: number) => {
        const shuffledArray = shuffleArray([...array]);
        return shuffledArray.slice(0, numItems);
    };
    const items = orders.map((order) => {
        const { orderDate, discount, discountCode, orderNumber, paymentDue,
        dateCreated, dateModified, createdBy, modifiedBy } = order;            ;
        const orderStatus = orderStatusTypes.find((x: { id: number }) => x.id === order.orderStatus);
        const orderedItem = orderedItems.find((x: { id: number }) => x.id === order.orderedItem);
        const paymentMethod = paymentMethods.find((x: { id: number }) => x.id === order.paymentMethod);
        const customer = customers.find((x: { id: number, address: number | unknown}) => x.id === order.customer) as { id: number, address: number | unknown};
        if (customer) {
            customer.address = postalAddresses.find((x: { id: number }) => x.id === customer.address);
        }
        // get 2 random payment methods
        const additionalPaymentMethods = getRandomItems(paymentMethods, 2);
        return {
            orderDate,
            discount,
            discountCode,
            orderNumber,
            paymentDue,
            orderStatus,
            orderedItem,
            paymentMethod,
            additionalPaymentMethods,
            customer,
            dateCreated,
            dateModified,
            createdBy,
            modifiedBy
        }
    });
    for (const item of items) {
        await db.executeAsync(new QueryExpression().insert(item).into(source), []);
    }
}

function onResolvingJsonMember(event: {target: QueryExpression; object?: string; member: string | MemberExpression; fullyQualifiedMember?: string;}) {
    const member = event.fullyQualifiedMember.split('.');
    const field = SimpleOrderSchema.fields.find((x) => x.name === member[0]);
    if (field == null) {
        return;
    }
    if (field.type !== 'Json') {
        return;
    }
    const targetWithCollection = event.target as { $collection?:string };
    event.object = targetWithCollection.$collection;
    // noinspection JSCheckFunctionSignatures
    event.member = new MethodCallExpression('jsonGet', [
        new MemberExpression(targetWithCollection.$collection + '.' + event.fullyQualifiedMember)
    ]);
}


describe('SelectJson', () => {

    let db: LocalSqlAdapter;
    beforeAll(async () => {
        const response = await fetch('http://localhost:3000/assets/db/local.db');
        const buffer = await response.arrayBuffer();
        db = new LocalSqlAdapter({
            buffer: new Uint8Array(buffer)
        });
        await createSimpleOrders(db);
    });

    it('should select json field', async () => {
        const Orders = new QueryEntity('SimpleOrders');
        const query = new QueryExpression();
        query.resolvingJoinMember.subscribe(onResolvingJsonMember);
        query.select((x: { id?: number, customer: { description: string } }) => {
            return {
                id: x.id,
                customer: x.customer.description
            }
        }).from(Orders);
        const results = await db.executeAsync<{ id?: number, customer: { description: string } }>(query, []);
        expect(results).toBeTruthy();
        for (const result of results) {
            expect(result).toBeTruthy();
            expect(result.id).toBeTruthy();
            expect(result.customer).toBeTruthy();
        }
    });

    it('should select nested json field', async () => {
        const Orders = new QueryEntity('SimpleOrders');
            const query = new QueryExpression();
            query.resolvingJoinMember.subscribe(onResolvingJsonMember);
            query.select((x: { id: number, customer: { description: string, address: { streetAddress: string } } }) => {
                // noinspection JSUnresolvedReference
                return {
                    id: x.id,
                    customer: x.customer.description,
                    address: x.customer.address.streetAddress
                }
            })
                .from(Orders);
            const results = await db.executeAsync<{id: number, customer: never}>(query, []);
            expect(results).toBeTruthy();
            for (const result of results) {
                expect(result).toBeTruthy();
                expect(result.id).toBeTruthy();
                expect(result.customer).toBeTruthy();
            }
    });

    it('should use jsonObject in ad-hoc queries', async () => {
        const Orders = 'OrderData';
        const Customers = 'PersonData';
        const OrderStatusTypes = 'OrderStatusTypeData';
        const q = new QueryExpression().select(
            'id', 'orderedItem', 'orderStatus', 'orderDate'
        ).from(Orders).join(new QueryEntity(Customers).as('customers')).with(
            new QueryExpression().where(
                new QueryField('customer').from(Orders)
            ).equal(
                new QueryField('id').from('customers')
            )
        ).join(new QueryEntity(OrderStatusTypes).as('orderStatusTypes')).with(
            new QueryExpression().where(
                new QueryField('orderStatus').from(Orders)
            ).equal(
                new QueryField('id').from('orderStatusTypes')
            )
        ).where(new QueryField('description').from('customers')).equal('Eric Thomas');
        const select = q.$select[Orders];
        select.push({
            customer: {
                $jsonObject: [
                    new QueryField('familyName').from('customers'),
                    new QueryField('givenName').from('customers'),
                ]
            }
        }, {
            orderStatus: {
                $jsonObject: [
                    new QueryField('name').from('orderStatusTypes'),
                    new QueryField('alternateName').from('orderStatusTypes'),
                ]
            }
        });
        const items = await db.executeAsync<{orderStatus: { name: string }, customer: {familyName: string, givenName: string}}>(q, []);
        expect(items).toBeTruthy();
        for (const item of items) {
            expect(item.customer).toBeTruthy();
            expect(item.customer.familyName).toEqual('Thomas');
            expect(item.customer.givenName).toEqual('Eric');
            expect(item.orderStatus).toBeTruthy();
            expect(item.orderStatus.name).toBeTruthy();
        }
    });

    it('should return json arrays', async () => {
        const Orders = 'OrderData';
        const People = 'PersonData';

        const queryPeople = new QueryExpression().select(
            'id', 'familyName', 'givenName', 'jobTitle', 'email'
        ).from(People);
        
        const queryOrders = new QueryExpression().select(
            'id', 'orderDate', 'orderStatus', 'orderedItem', 'customer'
        ).from(Orders);
        // prepare query for each customer
        queryOrders.where(
            new QueryField('customer').from(Orders)
        ).equal(
            new QueryField('id').from(People)
        );
        const selectPeople = queryPeople.$select[People];
        // add orders as json array
        selectPeople.push({
            orders: {
                $jsonArray: [
                    queryOrders
                ]
            }
        });
        const items = await db.executeAsync<{ id: number, orders: { customer: never }[] }>(queryPeople.take(10), []);
        expect(items.length).toBeTruthy();
        for (const item of items) {
            expect(Array.isArray(item.orders)).toBeTruthy();
            for (const order of item.orders) {
                expect(order.customer).toEqual(item.id);
            }

        }
    });

    it('should parse string as json array', async () => {
        const People = 'PersonData';
        const query = new QueryExpression().select(
            'id', 'familyName', 'givenName', 'jobTitle', 'email',
            new QueryField({
                tags: {
                    $jsonArray: [
                        new QueryField({
                            $value: '[ "user", "customer", "admin" ]'
                        })
                    ]
                }
            })
        ).from(People).where('email').equal('alexis.rees@example.com');
        const [item] = await db.executeAsync(query);
        expect(item).toBeTruthy();
    });

    it('should parse array as json array', async () => {
        // set context user
        const People = 'PersonData';
        const query = new QueryExpression().select(
            'id', 'familyName', 'givenName', 'jobTitle', 'email',
            new QueryField({
                tags: {
                    $jsonArray: [
                        {
                            $value: [ 'user', 'customer', 'admin' ]
                        }
                    ]
                }
            })
        ).from(People).where('email').equal('alexis.rees@example.com');
        const [item] = await db.executeAsync<{ tags: string[] }>(query);
        expect(item).toBeTruthy();
        expect(Array.isArray(item.tags)).toBeTruthy();
        expect(item.tags).toEqual([ 'user', 'customer', 'admin' ]);
    });


});