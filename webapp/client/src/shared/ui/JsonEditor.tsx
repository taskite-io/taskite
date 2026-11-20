/*
 * Copyright (c) 2026 lzgabel and Taskite contributors
 *
 * Licensed under the Taskite Community License, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     https://github.com/taskite-io/taskite/blob/main/LICENSE
 *
 * The Software is source-available for non-commercial use and limited evaluation
 * use. Other commercial use requires a separate written license. The Software is
 * provided "AS IS", without warranties or conditions of any kind.
 */
import React, {useMemo} from 'react';
import CodeMirror from '@uiw/react-codemirror';
import {json} from '@codemirror/lang-json';
import {EditorView} from '@codemirror/view';
import {Button} from 'antd';
import {parse, stringify} from 'lossless-json';
import {useConsoleTheme} from './ConsoleTheme';

/** Controlled JSON editor compatible with an Ant Design Form field. */
export function JsonEditor({value = '', onChange, disabled = false, readOnly = false, id, label = 'JSON 变量'}: {
  value?: string; onChange?: (value: string) => void; disabled?: boolean; readOnly?: boolean; id?: string; label?: string;
}) {
  const {dark} = useConsoleTheme();
  const extensions = useMemo(() => [json(), EditorView.lineWrapping,
    EditorView.contentAttributes.of({'aria-label': label, 'aria-multiline': 'true', spellcheck: 'false'})], [label]);
  let valid = true;
  try {parse(value.trim() || '{}');} catch {valid = false;}
  return <div className="json-editor" id={id} role="group" aria-label={`${label}编辑器`}>
    <div className="json-editor-toolbar">
      <span>JSON</span>
      {!readOnly && <Button size="small" type="text" disabled={disabled || !valid} onClick={() =>
        onChange?.(stringify(parse(value.trim() || '{}'), null, 2) || '{}')}>格式化</Button>}
    </div>
    <CodeMirror value={value} onChange={onChange} extensions={extensions}
      theme={dark ? 'dark' : 'light'} minHeight="150px" maxHeight="280px"
      editable={!disabled && !readOnly} readOnly={disabled || readOnly}
      basicSetup={{lineNumbers: true, foldGutter: true, highlightActiveLine: !readOnly,
        highlightActiveLineGutter: !readOnly, autocompletion: !readOnly}} />
  </div>;
}
