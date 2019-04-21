package org.whispersystems.textsecuregcm.liquibase;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.OutputStreamWriter;
import java.util.List;

import io.dropwizard.Configuration;
import io.dropwizard.db.DatabaseConfiguration;
import liquibase.Liquibase;

public class DbStatusCommand <T extends Configuration> extends AbstractLiquibaseCommand<T> {

  public DbStatusCommand(String migrations, DatabaseConfiguration<T> strategy, Class<T> configurationClass) {
    super("status", "Check for pending change sets.", migrations, strategy, configurationClass);
  }

  @Override
  public void configure(Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("-v", "--verbose")
             .action(Arguments.storeTrue())
             .dest("verbose")
             .help("Output verbose information");
    subparser.addArgument("-i", "--include")
             .action(Arguments.append())
             .dest("contexts")
             .help("include change sets from the given context");
  }

  @Override
  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public void run(Namespace namespace, Liquibase liquibase) throws Exception {
    liquibase.reportStatus(namespace.getBoolean("verbose"),
                           getContext(namespace),
                           new OutputStreamWriter(System.out, Charsets.UTF_8));
  }

  private String getContext(Namespace namespace) {
    final List<Object> contexts = namespace.getList("contexts");
    if (contexts == null) {
      return "";
    }
    return Joiner.on(',').join(contexts);
  }
}
