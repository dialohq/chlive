// The chat stack end to end, with its permissions in Postgres (`chat-smoke` in the chat shell,
// with `CHAT_SECURITY=postgres dnvr up`; WorkOS: smoke-workos.ts): what each user may read
// comes from Postgres, live: the messages of the authors they may see, the columns of the users
// they may read (`*` follows them; naming another is denied), and the tables (a lost table ends
// the subscription); anonymous connections read nothing.
import { as, closeAll, follow, has, message, MESSAGES } from "./live.ts";

const pg = process.env.PG_URL ?? "postgres://postgres@127.0.0.1:54329/chat";

function psql(sql: string) {
  const p = Bun.spawnSync(["psql", pg, "-v", "ON_ERROR_STOP=1", "-qc", sql]);
  if (p.exitCode !== 0) throw new Error(`psql: ${p.stderr.toString()}`);
}

// a fresh start, as seeded: Alice may not see Carol, nor the users' colours; Dave may not read
// messages
psql("delete from permissions where viewer_id = 'alice' and author_id = 'carol'");
psql("delete from column_permissions where user_id = 'alice' and column_name = 'color'");
psql("delete from table_permissions where user_id = 'dave' and table_name = 'messages'");

const alice = follow(as("alice"), MESSAGES);
const bob = follow(as("bob"), MESSAGES);
await alice.until(() => true, "alice's messages");
await bob.until(() => true, "bob's messages");

// a message from Carol: Bob is permitted to see her, Alice isn't
const body = await message("carol");
await bob.until(has(body), "Carol's message in Bob's chat");
await Bun.sleep(500);
if (has(body)(alice.state!)) throw new Error("Alice sees Carol's message without permission");
if (alice.state!.rows.some((r) => r.user_id === "carol")) throw new Error("Alice sees Carol's messages");
console.log("ok: Bob sees Carol's message, Alice doesn't");

// granted in Postgres: it comes into Alice's chat
psql("insert into permissions values ('alice', 'carol')");
await alice.until(has(body), "Carol's message in Alice's chat after the grant");
console.log("ok: granted, Alice sees it");

// revoked: it goes
psql("delete from permissions where viewer_id = 'alice' and author_id = 'carol'");
await alice.until((s) => !s.rows.some((r) => r.user_id === "carol"), "Carol's messages gone from Alice's chat");
console.log("ok: revoked, it's gone");

// columns: Carol reads the users' colours, Alice doesn't (`*` skips it, naming it is denied)
const USERS = "select * from users final order by user_id";
await follow(as("carol"), USERS).until((s) => s.rows.length >= 4 && "color" in s.rows[0], "Carol's users, coloured");
const aliceUsers = follow(as("alice"), USERS);
await aliceUsers.until((s) => s.rows.length >= 4 && !("color" in s.rows[0]), "Alice's users, without colours");
await follow(as("alice"), "select name, color from users final").fails("isn't readable here", "Alice naming color");
console.log("ok: columns: Carol sees colours, Alice doesn't, and can't name them");

// granted: Alice's live `*` gets them (a new snapshot); revoked: they go again
psql("insert into column_permissions values ('alice', 'users', 'color')");
await aliceUsers.until((s) => "color" in (s.rows[0] ?? {}), "colours in Alice's users after the grant");
psql("delete from column_permissions where user_id = 'alice' and column_name = 'color'");
await aliceUsers.until((s) => !("color" in (s.rows[0] ?? {})), "colours gone from Alice's users");
console.log("ok: a column granted and revoked live");

// tables: Dave may not read messages; granted, he may; revoked, his subscription ends
await follow(as("dave"), MESSAGES).fails("table messages isn't readable", "Dave's messages, denied");
psql("insert into table_permissions values ('dave', 'messages')");
// (a failed subscription is over: subscribe again)
const dave = follow(as("dave"), MESSAGES);
await dave.until((s) => s.rows.every((r) => r.user_id === "dave"), "Dave's messages after the grant");
psql("delete from table_permissions where user_id = 'dave' and table_name = 'messages'");
await dave.fails("table messages isn't readable", "Dave's live messages after the revoke");
console.log("ok: a table granted, then revoked live (the subscription ends)");

// anonymous: nothing
for (const sql of [MESSAGES, USERS]) {
  await follow(as(), sql).fails("needs a user", `anonymous ${sql}`);
}
console.log("ok: anonymous connections read nothing");

closeAll();
console.log("chat smoke: all ok");
