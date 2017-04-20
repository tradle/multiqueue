
# @tradle/multiqueue

[![Build Status](https://travis-ci.org/tradle/multiqueue.svg?branch=master)](https://travis-ci.org/tradle/multiqueue)

Persistent multiqueue, for parallelism between queues, and serial..ism within them. For example, you might have 100 people to whom you're sending messages concurrently (you're kind of a nag), but within each per-person queue, you want to send in series (so they don't find out you're kind of a nag).

## Usage 

### Basic

See [example](./example.js) for a basic lesson in nagging

### Self-ordering

The queues are self-ordering, provided you specify the position of each item.

See [example-order](./example-order.js) for a basic lesson in unscrambling eggs

## Todo

Improve accounting of queues, maybe using level-updown to atomically save a queue name with the append to changes-feed
