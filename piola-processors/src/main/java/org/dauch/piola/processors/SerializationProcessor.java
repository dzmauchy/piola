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
import javax.lang.model.element.*;
import javax.lang.model.type.*;
import javax.tools.Diagnostic;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.TreeMap;

import static java.util.stream.Collectors.joining;
import static javax.lang.model.element.ElementKind.ENUM;
import static javax.lang.model.element.ElementKind.RECORD;

public final class SerializationProcessor extends BaseProcessor {

  @Override
  public Set<String> getSupportedAnnotationTypes() {
    return Set.of("org.dauch.piola.io.annotation.Serde");
  }

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
    boolean result = false;
    for (var t : roundEnv.getElementsAnnotatedWithAny(annotations.toArray(TypeElement[]::new))) {
      if (t instanceof TypeElement te && t.getKind() == RECORD) {
        try {
          process(te);
        } catch (Throwable e) {
          messager.printError(e.toString(), t);
        }
        result = true;
      }
    }
    return result;
  }

  private void process(TypeElement t) throws Exception {
    var fqn = t.getQualifiedName().toString();
    var src = filer.createSourceFile("org.dauch.piola.io.api.serde." + t.getSimpleName() + "Serde", t);
    var cs = t.getRecordComponents();
    var is = new TreeMap<Integer, RecordComponentElement>();
    cs.forEach(c -> {
      var id = id(c);
      var old = is.put(id, c);
      if (old != null) {
        throw new IllegalStateException("Duplicate record component id: " + id);
      }
    });
    try (var w = new PrintWriter(src.openWriter())) {
      w.printf("package org.dauch.piola.io.api.serde;%n%n");
      w.printf("import %s;%n", ByteBuffer.class.getName());
      w.printf("import org.dauch.piola.io.api.SerializationContext;%n");
      w.printf("import %s;%n", fqn);
      w.printf("import static %s.*;%n", fqn);
      w.printf("import org.dauch.piola.io.api.Serialization;%n");
      w.printf("public class %s {%n", t.getSimpleName() + "Serde");
      w.printf("  public static %s read(ByteBuffer $b, SerializationContext $c) {%n", t.getSimpleName());
      for (var cmp: cs) {
        w.printf("    %s %s = %s;%n", cmp.asType().toString(), cmp.getSimpleName(), defaultValue(cmp));
      }
      w.printf("    while (true) {%n");
      w.printf("      var f = Byte.toUnsignedInt($b.get());%n");
      w.printf("      switch (f) {%n");
      w.printf("        case 0: return new %s(%s);%n", fqn, cs.stream().map(this::rec).collect(joining(",")));
      is.forEach((id, cmp) -> {
        w.printf("        case %d:%n", id);
        if (cmp.asType() instanceof DeclaredType dt && isSerde(dt, cmp)) {
          w.printf("          %s = %sSerde.read($b, $c.child(%d));%n", cmp.getSimpleName(), dt.asElement().getSimpleName(), id);
        } else {
          w.printf("          %s = Serialization.read($b, %s);%n", cmp.getSimpleName(), cmp.getSimpleName());
        }
        w.printf("          break;%n");
      });
      w.printf("        default:%n");
      w.printf("          $c.addUnknownField(f);%n");
      w.printf("          break;%n");
      w.printf("      }%n");
      w.printf("    }%n");
      w.printf("  }%n");
      w.printf("  public static void write(%s $r, ByteBuffer $b) {%n", t.getSimpleName());
      for (var cmp: cs) {
        if (cmp.asType() instanceof DeclaredType) {
          w.printf("    if ($r.%s() != null) {%n  ", cmp.getSimpleName());
        }
        if (cmp.asType() instanceof DeclaredType dt && isSerde(dt, cmp)) {
          w.printf("    %sSerde.write($r.%s(), $b.put((byte) %d));%n", dt.asElement().getSimpleName(), cmp.getSimpleName(), id(cmp));
        } else {
          w.printf("    Serialization.write($b.put((byte) %d), $r.%s());%n", id(cmp), cmp.getSimpleName());
        }
        if (cmp.asType() instanceof DeclaredType) {
          w.printf("    }%n");
        }
      }
      w.printf("    $b.put((byte) 0);%n");
      w.printf("  }%n");
      w.printf("}");
    }
    messager.printMessage(Diagnostic.Kind.NOTE, "Processed", t);
  }

  private static String stub(TypeMirror type) {
    return switch (type.getKind()) {
      case INT -> "0";
      case LONG -> "0L";
      case DOUBLE -> "0d";
      case BOOLEAN -> "false";
      case BYTE -> "(byte) 0";
      case SHORT -> "(short) 0";
      case CHAR -> "'0'";
      case FLOAT -> "0f";
      case VOID -> throw new IllegalStateException();
      default -> "(" + type + ") null";
    };
  }

  static String defaultValue(RecordComponentElement el) {
    return el.getAnnotationMirrors().stream()
      .filter(a -> a.getAnnotationType().asElement().getSimpleName().contentEquals("Default"))
      .map(a -> (String) a.getElementValues().values().iterator().next().getValue())
      .findFirst()
      .map(v -> {
        if (el.asType() instanceof DeclaredType t && t.asElement() instanceof TypeElement te && te.getKind() == ENUM) {
          return te.getQualifiedName().toString() + "." + v;
        } else {
          return v;
        }
      })
      .orElseGet(() -> stub(el.asType()));
  }

  private int id(RecordComponentElement el) {
    return el.getAnnotationMirrors().stream()
      .filter(a -> a.getAnnotationType().asElement().getSimpleName().contentEquals("Id"))
      .mapToInt(a -> Byte.toUnsignedInt((byte) a.getElementValues().values().iterator().next().getValue()))
      .findFirst()
      .orElseThrow(() ->
        new IllegalArgumentException("No ID annotation found on " + el.getSimpleName() + "." + el.getSimpleName())
      );
  }

  private String rec(RecordComponentElement el) {
    return el.getSimpleName().toString();
  }

  private boolean isSerde(DeclaredType dt, RecordComponentElement e) {
    var element = types.asElement(dt);
    if (element.getKind() != RECORD) {
      return false;
    }
    for (var m : e.getAnnotationMirrors()) {
      if (m.getAnnotationType().asElement().getSimpleName().contentEquals("Custom")) {
        return false;
      }
    }
    return true;
  }
}
