# Domains vs promises

## Why domains are not a good solution

The Node Foundation did [deprecate][domain-deprecate] the domain API,
and for good reasons. The domains were mainly here to ensure that in
case of a thrown error, in the context of an http server, you would
not leave sockets hanging, and plainly stop every single request still
being processed asynchronously. This is due to the fact that error
handling in node is [far from perfect][error-handling] and hard to set
up. It was a hack intended to be dropped as soon as possible.

## Promises

Promises allow for contained throws that will not contaminate other
asynchronous operations. The `catch()` method will allow you to deal
with your exceptions gracefully.

## Code samples

### Domains

```es6
http.createServer((req, res) => {
    const d = domain.create();
    d.on('error', yourRecoveryFunction)
    d.run(yourHandler(req, res));
});
```

### Promises

```es6
http.createServer((req, res) => {
    Promise.resolve(yourHandler(req, res)).catch(yourRecoveryFunction);
});
```

[domain-deprecate]: https://github.com/nodejs/node/issues/66
[error-handling]: https://www.joyent.com/developers/node/design/errors
