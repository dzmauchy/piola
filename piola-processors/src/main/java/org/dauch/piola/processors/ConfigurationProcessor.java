package org.dauch.piola.processors;

/*-
 * #%L
 * piola-processors
 * %%
 * Copyright (C) 2024 dauch
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-3.0.html>.
 * #L%
 */

import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import java.io.PrintWriter;
import java.util.Properties;
import java.util.Set;

public final class ConfigurationProcessor extends BaseProcessor {

  @Override
  public Set<String> getSupportedAnnotationTypes() {
    return Set.of("org.dauch.piola.io.annotation.Conf");
  }

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
    boolean result = false;
    for (var t : roundEnv.getElementsAnnotatedWithAny(annotations.toArray(TypeElement[]::new))) {
      if (t instanceof TypeElement te && t.getKind() == ElementKind.RECORD) {
        try {
          process(te);
        } catch (Throwable e) {
          messager.printError("Configuration error %s".formatted(e), t);
        }
        result = true;
      }
    }
    return result;
  }

  private void process(TypeElement t) throws Exception {
    var fqn = t.getQualifiedName().toString();
    var src = filer.createSourceFile("org.dauch.piola.io.api.conf." + t.getSimpleName() + "IO", t);
    var cs = t.getRecordComponents();
    try (var w = new PrintWriter(src.openWriter())) {
      w.printf("package org.dauch.piola.io.api.conf;%n%n");
      w.printf("import org.dauch.piola.util.Props;%n");
      w.printf("import %s;%n", fqn);
      w.printf("import %s;%n", Properties.class.getName());
      w.printf("import static %s.*;%n", fqn);
      w.printf("public class %s {%n", t.getSimpleName() + "IO");
      w.printf("  public static %s get(String prefix, Properties properties) {%n", t.getSimpleName());
      w.printf("    var $props = new Props(properties);%n");
      for (var c : cs) {
        var dv = SerializationProcessor.defaultValue(c);
        w.printf("    var %s = $props.get(prefix + \".%s\", %s);%n", c.getSimpleName(), c.getSimpleName(), dv);
      }
      w.printf("    return new %s(%n", t.getSimpleName());
      for (var it = cs.iterator(); it.hasNext(); ) {
        var c = it.next();
        w.printf("      %s%s%n", c.getSimpleName(), it.hasNext() ? "," : "");
      }
      w.printf("    );%n");
      w.printf("  }%n");
      w.printf("}");
    }
    messager.printMessage(Diagnostic.Kind.NOTE, "Processed", t);
  }
}
