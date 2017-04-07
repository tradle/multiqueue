
# @tradle/multiqueue

Persistent multiqueue, for parallelism between queues, and serial..ism within them. For example, you might have 100 people to whom you're sending messages concurrently (you're kind of a nag), but within each per-person queue, you want to send in series (so they don't find out you're kind of a nag).

## Usage 

See [example](./example.js) for a lesson in nagging
