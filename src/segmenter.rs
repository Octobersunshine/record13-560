use uuid::Uuid;
use crate::models::{LiveScript, ScriptSegment, SegmentBy};

pub fn parse_script(
    title: &str,
    content: &str,
    default_duration_ms: u64,
    segment_by: SegmentBy,
) -> LiveScript {
    let raw_segments = match segment_by {
        SegmentBy::Paragraph => split_by_paragraph(content),
        SegmentBy::Sentence => split_by_sentence(content),
        SegmentBy::Newline => split_by_newline(content),
        SegmentBy::FixedLength => split_by_fixed_length(content, 50),
    };

    let segments: Vec<ScriptSegment> = raw_segments
        .into_iter()
        .enumerate()
        .map(|(index, text)| ScriptSegment {
            id: Uuid::new_v4(),
            index,
            text: text.trim().to_string(),
            duration_ms: estimate_duration(&text, default_duration_ms),
        })
        .filter(|s| !s.text.is_empty())
        .collect();

    let total_duration_ms = segments.iter().map(|s| s.duration_ms).sum();

    LiveScript {
        id: Uuid::new_v4(),
        title: title.to_string(),
        created_at: chrono::Utc::now(),
        segments,
        total_duration_ms,
    }
}

fn split_by_paragraph(content: &str) -> Vec<String> {
    content
        .split("\n\n")
        .map(|s| s.to_string())
        .filter(|s| !s.trim().is_empty())
        .collect()
}

fn split_by_newline(content: &str) -> Vec<String> {
    content
        .lines()
        .map(|s| s.to_string())
        .filter(|s| !s.trim().is_empty())
        .collect()
}

fn split_by_sentence(content: &str) -> Vec<String> {
    let mut segments = Vec::new();
    let mut current = String::new();

    let chars: Vec<char> = content.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        current.push(c);

        if matches!(c, '。' | '！' | '？' | '.' | '!' | '?') {
            if i + 1 < chars.len() {
                let next = chars[i + 1];
                if next == ' ' || next == '\n' || next == '　' {
                    if !current.trim().is_empty() {
                        segments.push(current.trim().to_string());
                    }
                    current.clear();
                }
            } else {
                if !current.trim().is_empty() {
                    segments.push(current.trim().to_string());
                }
                current.clear();
            }
        }
        i += 1;
    }

    if !current.trim().is_empty() {
        segments.push(current.trim().to_string());
    }

    if segments.is_empty() {
        segments.push(content.trim().to_string());
    }

    segments
}

fn split_by_fixed_length(content: &str, max_chars: usize) -> Vec<String> {
    let mut segments = Vec::new();
    let chars: Vec<char> = content.chars().collect();
    let mut start = 0;

    while start < chars.len() {
        let mut end = (start + max_chars).min(chars.len());

        if end < chars.len() {
            while end > start && !chars[end - 1].is_whitespace()
                && !matches!(chars[end - 1], '。' | '，' | '、' | '.' | ',' | ';' | '；')
            {
                end -= 1;
            }
            if end == start {
                end = (start + max_chars).min(chars.len());
            }
        }

        let segment: String = chars[start..end].iter().collect();
        if !segment.trim().is_empty() {
            segments.push(segment.trim().to_string());
        }
        start = end;
    }

    segments
}

fn estimate_duration(text: &str, default_duration_ms: u64) -> u64 {
    let char_count = text.chars().filter(|c| !c.is_whitespace()).count();
    let chinese_chars = text.chars().filter(|c| {
        ('\u{4E00}'..='\u{9FFF}').contains(c)
            || ('\u{3000}'..='\u{303F}').contains(c)
            || ('\u{FF00}'..='\u{FFEF}').contains(c)
    }).count();

    let avg_chars_per_second = if chinese_chars > char_count / 2 {
        3.5
    } else {
        4.0
    };

    if char_count == 0 {
        return default_duration_ms;
    }

    let estimated = (char_count as f64 / avg_chars_per_second * 1000.0) as u64;
    estimated.max(1500).min(30000)
}
