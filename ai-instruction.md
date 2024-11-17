In this stage, you'll add support for queuing commands within a transaction.

Queuing commands
After MULTI is executed, any further commands from a connection are queued until EXEC is executed.

The response to all of these commands is +QUEUED\r\n (That's QUEUED encoded as a Simple String).

Example:

$ redis-cli
> MULTI
OK
> SET foo 41
QUEUED
> INCR foo
QUEUED

... (and so on, until EXEC is executed)
When commands are queued, they should not be executed or alter the database in any way.

In the example above, until EXEC is executed, the key foo will not exist.

Tests
The tester will execute your program like this:

$ ./your_program.sh
The tester will then connect to your server as a Redis client, and send multiple commands using the same connection:

$ redis-cli
> MULTI
> SET foo 41 (expecting "+QUEUED\r\n")
> INCR foo (expecting "+QUEUED\r\n")
Since these commands were only "queued", the key foo should not exist yet. The tester will verify this by creating another connection and sending this command:

$ redis-cli GET foo (expecting `$-1\r\n` as the response)
