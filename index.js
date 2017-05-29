const nedbTarget = require("./nedbTarget.js");
const parseArgs = require("minimist");

const USAGE = `
Usage: nedb-target <options> [dir]

Populates nedb collections from Singer.io streams, one collection per type.

Options:
  --echo       Echo stdin to stdout    [boolean]
  --help, -h   Print this message      [boolean]

`;

const argv = parseArgs(process.argv.slice(2), {
  boolean: ["echo", "help", "h"]
});

if (argv._.length > 1 || argv.help || argv.h) {
  process.stdout.write(USAGE);
  process.exit(1);
}

try {
  nedbTarget({
    dbBasePath: argv._[0],
    echo: argv.echo
  });
} catch (error) {
  process.stdout.write(error + "\n");
  process.exit(2);
}
