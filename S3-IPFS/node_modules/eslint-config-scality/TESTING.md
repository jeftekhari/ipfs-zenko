# Test layout in the projects

Guidelines surrounding our testing setup

## Types of tests

During the growth of our project, we identified four different kind of testing.
These vary in size, but they can be separated in two categories: *small tests*
and **big tests**.

### Small tests

They should be easy to run, using simple npm scripts, to allow for a quick local
testing. They are launched by a vanilla Circle-CI every time you push a branch
and will not require [CI]. That means they are expected to be found in
the `tests` directory of the related project.

#### Unit tests

Those are the smallest tests possible. They do not require to run an instance
of the actual program, and should assess the good behaviour of every function.
They can be run using the `npm test` command, and are written using mocha.

The role of this type of test is to validate the logic of a written piece of
code. They cover expected usage as well as as many unexpected uses as possible.

Belong to the `tests/unit` directory.

#### Functional tests

These tests encompass the whole program, thus need a running instance of it.
They can be launched with the `npm run ft_test` command, and will require the
program to be started using `npm start` beforehand. They may require a few
dependencies (S3 Clients, supervisord, ...).

Belong to the `tests/functional` directory.

### Big tests

They are meant to test heavily a single component, or test the entire project
(end-to-end testing). Those are not trivial to set-up, and are only started
on an @ironman-machine invocation. That invocation starts a build using
[Integration] master branch, using the branch related to the ongoing PR
for each project. They are thus expected to be found in [Integration].

#### Integration tests

These tests are still only assessing a single module of the project. Being
non-trivial, they require a more advanced setup usually covered by [CI]
using `Dockerfile`s and `docker-compose`.

#### End-to-end tests

These tests ensure no breakage in the chain, using our deployment setup,
[Federation]. A small dependency patcher will be used to ensure testing
of a supposedly non-breaking setup.

[CI]: https://github.com/scality/CI
[Integration]: https://github.com/scality/Integration
[Federation]: https://github.com/scality/Federation
