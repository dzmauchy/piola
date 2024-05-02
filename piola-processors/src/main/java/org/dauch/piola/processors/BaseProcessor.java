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

import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.*;
import javax.lang.model.util.Types;
import java.util.Set;

public abstract class BaseProcessor implements Processor {

  protected Filer filer;
  protected Messager messager;
  protected Types types;

  @Override
  public Set<String> getSupportedOptions() {
    return Set.of();
  }

  @Override
  public SourceVersion getSupportedSourceVersion() {
    return SourceVersion.RELEASE_22;
  }

  @Override
  public void init(ProcessingEnvironment processingEnv) {
    filer = processingEnv.getFiler();
    messager = processingEnv.getMessager();
    types = processingEnv.getTypeUtils();
  }

  @Override
  public Set<Completion> getCompletions(Element e, AnnotationMirror ann, ExecutableElement member, String userText) {
    return Set.of();
  }
}
